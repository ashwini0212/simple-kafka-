package com.simplekafka.broker;

import com.simplekafka.protocol.CreateTopicRequest;
import com.simplekafka.protocol.CreateTopicResponse;
import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.TopicPartition;
import com.simplekafka.zk.ZkPaths;
import com.simplekafka.zk.ZookeeperClient;
import org.apache.zookeeper.KeeperException;
import org.apache.zookeeper.Op;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.regex.Pattern;

/**
 * Exactly one broker in the cluster is the controller: whoever manages to
 * create the ephemeral {@code /controller} node first. If it dies the node
 * vanishes, every broker's watch fires, and the race starts again.
 *
 * The controller:
 * <ul>
 *   <li>assigns partitions and replicas to brokers when a topic is created,</li>
 *   <li>picks a new leader (from the ISR) for every partition whose leader died,</li>
 *   <li>removes dead brokers from ISRs.</li>
 * </ul>
 * It does all of this by writing partition state nodes; brokers watch those
 * nodes and change role accordingly.
 */
public class Controller {
    private static final Logger LOG = LoggerFactory.getLogger(Controller.class);
    private static final Pattern TOPIC_NAME = Pattern.compile("[a-zA-Z0-9._-]{1,200}");

    private final int brokerId;
    private final ZookeeperClient zk;
    private final MetadataCache cache;
    private final ZkPartitionStates zkStates;
    private final boolean uncleanLeaderElection = Boolean.getBoolean("simplekafka.unclean.leader.election");
    private final Random random = new Random();
    private volatile boolean active;

    public Controller(int brokerId, ZookeeperClient zk, MetadataCache cache, ZkPartitionStates zkStates) {
        this.brokerId = brokerId;
        this.zk = zk;
        this.cache = cache;
        this.zkStates = zkStates;
    }

    public boolean isActive() {
        return active;
    }

    /** Try to become controller. Returns true if we won. */
    public boolean tryElect() throws KeeperException, InterruptedException {
        if (!zk.createEphemeral(ZkPaths.CONTROLLER, Integer.toString(brokerId).getBytes(StandardCharsets.UTF_8))) {
            return false;
        }
        active = true;
        cache.controllerId = brokerId;
        LOG.info("Broker {} won the election and is now the CONTROLLER", brokerId);
        // The previous controller may have died mid-way; make sure every partition has a live leader.
        reconcileAll(Collections.emptySet());
        return true;
    }

    public void resign() {
        if (active) {
            LOG.info("Broker {} is no longer the controller", brokerId);
        }
        active = false;
    }

    /**
     * Brokers joined, left, or restarted (bounced) since the last check. A bounced
     * broker is first treated as dead so it loses leadership and rejoins as a follower.
     */
    public void onBrokersChanged(Set<Integer> joined, Set<Integer> left, Set<Integer> bounced)
            throws KeeperException, InterruptedException {
        if (!active) {
            return;
        }
        if (!left.isEmpty()) {
            LOG.info("Controller: brokers {} failed, electing new leaders where needed", left);
        }
        if (!bounced.isEmpty()) {
            reconcileAll(bounced);
        }
        reconcileAll(Collections.emptySet());
    }

    private void reconcileAll(Set<Integer> exclude) throws KeeperException, InterruptedException {
        for (Map.Entry<String, Map<Integer, List<Integer>>> topic : cache.assignments.entrySet()) {
            for (int p : topic.getValue().keySet()) {
                reconcile(new TopicPartition(topic.getKey(), p), exclude);
            }
        }
    }

    /** Makes sure the partition's leader is alive and its ISR only lists live brokers. */
    private void reconcile(TopicPartition tp, Set<Integer> exclude) throws KeeperException, InterruptedException {
        List<Integer> replicas = cache.replicas(tp);
        Set<Integer> live = new HashSet<>(cache.liveBrokers.keySet());
        live.removeAll(exclude);
        PartitionState before = zkStates.read(tp);
        PartitionState after = zkStates.update(tp, current -> {
            List<Integer> liveIsr = new ArrayList<>();
            for (int r : current.isr) {
                if (live.contains(r)) {
                    liveIsr.add(r);
                }
            }
            if (current.leader >= 0 && live.contains(current.leader)) {
                return current.withIsr(liveIsr);
            }
            // Leader is gone: prefer the first live replica that is in sync (no data loss).
            for (int r : replicas) {
                if (liveIsr.contains(r)) {
                    return current.withNewLeader(r, liveIsr);
                }
            }
            if (uncleanLeaderElection) {
                for (int r : replicas) {
                    if (live.contains(r)) {
                        LOG.warn("{}: UNCLEAN election of out-of-sync broker {}, committed messages may be lost", tp, r);
                        return current.withNewLeader(r, Collections.singletonList(r));
                    }
                }
            }
            // Nobody in sync is alive. Keep the last ISR so we know who to wait for.
            return current.leader == -1 ? current : current.withNewLeader(-1, current.isr);
        });
        if (before != null && after != null && !before.sameAs(after)) {
            if (before.leader != after.leader) {
                LOG.info("Controller: {} leader {} -> {} (epoch {}, isr {})",
                        tp, before.leader, after.leader, after.leaderEpoch, after.isr);
            } else {
                LOG.info("Controller: {} isr {} -> {}", tp, before.isr, after.isr);
            }
        }
    }

    public synchronized CreateTopicResponse createTopic(CreateTopicRequest request)
            throws KeeperException, InterruptedException {
        if (!active) {
            return new CreateTopicResponse(ErrorCode.NOT_CONTROLLER, "Broker " + brokerId + " is not the controller");
        }
        String topic = request.topic();
        if (topic == null || !TOPIC_NAME.matcher(topic).matches()) {
            return new CreateTopicResponse(ErrorCode.INVALID_REQUEST, "Topic names may only use letters, digits, '.', '_' and '-'");
        }
        if (request.numPartitions() < 1) {
            return new CreateTopicResponse(ErrorCode.INVALID_PARTITIONS, "Need at least one partition");
        }
        List<Integer> brokers = new ArrayList<>(cache.liveBrokers.keySet());
        Collections.sort(brokers);
        int rf = request.replicationFactor();
        if (rf < 1 || rf > brokers.size()) {
            return new CreateTopicResponse(ErrorCode.INVALID_REPLICATION_FACTOR,
                    "Replication factor " + rf + " but only " + brokers.size() + " brokers are alive");
        }

        // Round-robin: partition p's replicas are the rf brokers starting at (start + p).
        // This spreads both replicas and preferred leaders evenly across brokers.
        int start = random.nextInt(brokers.size());
        Map<Integer, List<Integer>> assignment = new TreeMap<>();
        for (int p = 0; p < request.numPartitions(); p++) {
            List<Integer> replicas = new ArrayList<>();
            for (int i = 0; i < rf; i++) {
                replicas.add(brokers.get((start + p + i) % brokers.size()));
            }
            assignment.put(p, replicas);
        }

        // Write the assignment and the initial leader of every partition in one atomic multi-op.
        List<Op> ops = new ArrayList<>();
        ops.add(ZookeeperClient.createOp(ZkPaths.topic(topic), TopicAssignment.toZkData(assignment)));
        ops.add(ZookeeperClient.createOp(ZkPaths.partitions(topic), new byte[0]));
        for (Map.Entry<Integer, List<Integer>> e : assignment.entrySet()) {
            List<Integer> replicas = e.getValue();
            PartitionState initial = new PartitionState(replicas.get(0), 0, replicas, 0);
            ops.add(ZookeeperClient.createOp(ZkPaths.partition(topic, e.getKey()), new byte[0]));
            ops.add(ZookeeperClient.createOp(ZkPaths.partitionState(topic, e.getKey()), initial.toZkData()));
        }
        try {
            zk.multi(ops);
        } catch (KeeperException.NodeExistsException e) {
            return new CreateTopicResponse(ErrorCode.TOPIC_ALREADY_EXISTS, "Topic " + topic + " already exists");
        }
        LOG.info("Controller: created topic {} with {} partitions, replicas {}", topic, assignment.size(), assignment);
        return new CreateTopicResponse(ErrorCode.NONE, "Created topic " + topic);
    }
}
