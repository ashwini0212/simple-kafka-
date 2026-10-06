package com.simplekafka.broker;

import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.Message;
import com.simplekafka.protocol.ProduceRequest;
import com.simplekafka.protocol.ProduceResponse;
import com.simplekafka.protocol.TopicPartition;
import com.simplekafka.storage.Log;
import org.apache.zookeeper.KeeperException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * One replica of a partition on this broker: its log plus the replication
 * bookkeeping for whichever role (leader or follower) this broker plays.
 *
 * <p>As leader it:
 * <ul>
 *   <li>appends produced messages and assigns offsets,</li>
 *   <li>tracks how far each follower has fetched (its log end offset),</li>
 *   <li>advances the high watermark to the smallest log end offset in the ISR,</li>
 *   <li>adds followers to the ISR once they catch up and drops ones that fall behind.</li>
 * </ul>
 *
 * <p>As follower it truncates to its high watermark when it starts following a
 * new leader, then appends whatever the replica fetcher brings back.
 *
 * <p>Every method synchronizes on the partition; producers waiting for acks=all
 * and consumers long-polling use wait/notifyAll on the same monitor.
 */
public class Partition {
    private static final Logger LOG = LoggerFactory.getLogger(Partition.class);

    private final TopicPartition tp;
    private final int brokerId;
    private final Log log;
    private final ZkPartitionStates zkStates;
    private final long replicaLagTimeMs;

    private PartitionState state;
    private List<Integer> replicas = Collections.emptyList();
    private boolean leader;

    // Leader-only bookkeeping about each follower.
    private final Map<Integer, Long> followerLogEndOffset = new HashMap<>();
    private final Map<Integer, Long> lastCaughtUpTimeMs = new HashMap<>();
    private final Map<Integer, Long> leaderLeoAtLastFetch = new HashMap<>();
    private final Map<Integer, Long> lastFetchTimeMs = new HashMap<>();

    public Partition(TopicPartition tp, int brokerId, Log log, ZkPartitionStates zkStates, long replicaLagTimeMs) {
        this.tp = tp;
        this.brokerId = brokerId;
        this.log = log;
        this.zkStates = zkStates;
        this.replicaLagTimeMs = replicaLagTimeMs;
    }

    public TopicPartition topicPartition() {
        return tp;
    }

    public Log log() {
        return log;
    }

    public synchronized boolean isLeader() {
        return leader;
    }

    public synchronized PartitionState state() {
        return state;
    }

    private boolean isStale(PartitionState s) {
        return state != null && s.zkVersion < state.zkVersion;
    }

    public synchronized void makeLeader(PartitionState s, List<Integer> replicas) {
        if (isStale(s)) {
            return;
        }
        boolean newTerm = !leader || state == null || s.leaderEpoch != state.leaderEpoch;
        this.state = s;
        this.replicas = replicas;
        this.leader = true;
        if (newTerm) {
            followerLogEndOffset.clear();
            leaderLeoAtLastFetch.clear();
            lastFetchTimeMs.clear();
            long now = System.currentTimeMillis();
            for (int r : replicas) {
                lastCaughtUpTimeMs.put(r, now);
            }
            LOG.info("{}: became LEADER at epoch {} (log end offset {}, high watermark {}, isr {})",
                    tp, s.leaderEpoch, log.logEndOffset(), log.highWatermark(), s.isr);
        }
        maybeIncrementHighWatermark();
        notifyAll();
    }

    public synchronized void makeFollower(PartitionState s, List<Integer> replicas) throws IOException {
        if (isStale(s)) {
            return;
        }
        boolean newTerm = leader || state == null || s.leaderEpoch != state.leaderEpoch || s.leader != state.leader;
        this.state = s;
        this.replicas = replicas;
        this.leader = false;
        if (newTerm) {
            // Anything past the high watermark may never have been committed. Drop it
            // and re-fetch from the new leader, so our log matches the leader's.
            long oldEnd = log.logEndOffset();
            long hw = log.highWatermark();
            log.truncateTo(hw);
            LOG.info("{}: became FOLLOWER of broker {} at epoch {} (truncated log end offset {} -> {})",
                    tp, s.leader, s.leaderEpoch, oldEnd, log.logEndOffset());
        }
        notifyAll();
    }

    // ---------------------------------------------------------------- leader side

    public synchronized ProduceResponse appendAsLeader(ProduceRequest request) throws IOException, InterruptedException {
        if (!leader) {
            return new ProduceResponse(ErrorCode.NOT_LEADER_FOR_PARTITION, -1L);
        }
        List<Message> messages = request.messages();
        long baseOffset = log.append(messages);
        long requiredHighWatermark = baseOffset + messages.size();
        maybeIncrementHighWatermark();
        notifyAll();
        if (request.acks() != ProduceRequest.ACKS_ALL) {
            return new ProduceResponse(ErrorCode.NONE, baseOffset);
        }
        // acks=all: wait until every in-sync replica has the messages,
        // i.e. the high watermark has moved past them.
        long deadline = System.currentTimeMillis() + request.timeoutMs();
        while (log.highWatermark() < requiredHighWatermark) {
            if (!leader) {
                return new ProduceResponse(ErrorCode.NOT_LEADER_FOR_PARTITION, baseOffset);
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return new ProduceResponse(ErrorCode.REQUEST_TIMED_OUT, baseOffset);
            }
            wait(remaining);
        }
        return new ProduceResponse(ErrorCode.NONE, baseOffset);
    }

    public synchronized FetchResponse fetch(FetchRequest request) throws IOException, InterruptedException {
        if (!leader) {
            return FetchResponse.error(ErrorCode.NOT_LEADER_FOR_PARTITION);
        }
        boolean fromFollower = request.replicaId() >= 0;
        if (fromFollower) {
            updateFollowerState(request.replicaId(), request.fetchOffset());
        }
        long offset = request.fetchOffset();
        long deadline = System.currentTimeMillis() + request.maxWaitMs();
        while (true) {
            long leo = log.logEndOffset();
            long hw = log.highWatermark();
            if (offset > leo || offset < log.logStartOffset()) {
                return new FetchResponse(ErrorCode.OFFSET_OUT_OF_RANGE, hw, leo, Collections.emptyList());
            }
            // Consumers may only read committed messages; followers read everything.
            long limit = fromFollower ? leo : hw;
            if (offset < limit) {
                return new FetchResponse(ErrorCode.NONE, hw, leo, log.read(offset, request.maxBytes(), limit));
            }
            long remaining = deadline - System.currentTimeMillis();
            if (remaining <= 0) {
                return new FetchResponse(ErrorCode.NONE, hw, leo, Collections.emptyList());
            }
            wait(remaining);
            if (!leader) {
                return FetchResponse.error(ErrorCode.NOT_LEADER_FOR_PARTITION);
            }
        }
    }

    /** A follower's fetch offset is exactly its log end offset: everything before it is replicated. */
    private void updateFollowerState(int replicaId, long fetchOffset) {
        long now = System.currentTimeMillis();
        long leo = log.logEndOffset();
        followerLogEndOffset.put(replicaId, fetchOffset);

        // "Caught up" means it reached our log end, either now or as of its previous fetch.
        Long previousLeo = leaderLeoAtLastFetch.get(replicaId);
        if (fetchOffset >= leo) {
            lastCaughtUpTimeMs.put(replicaId, now);
        } else if (previousLeo != null && fetchOffset >= previousLeo) {
            lastCaughtUpTimeMs.merge(replicaId, lastFetchTimeMs.get(replicaId), Math::max);
        }
        leaderLeoAtLastFetch.put(replicaId, leo);
        lastFetchTimeMs.put(replicaId, now);

        if (!state.isr.contains(replicaId) && replicas.contains(replicaId) && fetchOffset >= log.highWatermark()) {
            List<Integer> newIsr = new ArrayList<>(state.isr);
            newIsr.add(replicaId);
            LOG.info("{}: broker {} caught up, expanding ISR {} -> {}", tp, replicaId, state.isr, newIsr);
            updateIsr(newIsr);
        }
        maybeIncrementHighWatermark();
    }

    /** HW = the smallest log end offset among the in-sync replicas. */
    private void maybeIncrementHighWatermark() {
        if (!leader) {
            return;
        }
        long current = log.highWatermark();
        long newHw = log.logEndOffset();
        for (int r : state.isr) {
            if (r != brokerId) {
                newHw = Math.min(newHw, followerLogEndOffset.getOrDefault(r, current));
            }
        }
        if (newHw > current) {
            log.setHighWatermark(newHw);
            notifyAll();
        }
    }

    /** Called periodically: drop followers that have not caught up within replicaLagTimeMs. */
    public synchronized void maybeShrinkIsr() {
        if (!leader) {
            return;
        }
        long now = System.currentTimeMillis();
        List<Integer> newIsr = new ArrayList<>();
        List<Integer> laggards = new ArrayList<>();
        for (int r : state.isr) {
            if (r != brokerId && now - lastCaughtUpTimeMs.getOrDefault(r, 0L) > replicaLagTimeMs) {
                laggards.add(r);
            } else {
                newIsr.add(r);
            }
        }
        if (!laggards.isEmpty()) {
            LOG.info("{}: brokers {} fell behind, shrinking ISR {} -> {}", tp, laggards, state.isr, newIsr);
            if (updateIsr(newIsr)) {
                maybeIncrementHighWatermark();
            }
        }
    }

    /** Leader writes the new ISR to ZooKeeper, but only if it is still the leader for this epoch. */
    private boolean updateIsr(List<Integer> newIsr) {
        final int epoch = state.leaderEpoch;
        try {
            PartitionState written = zkStates.update(tp, current ->
                    current.leader == brokerId && current.leaderEpoch == epoch ? current.withIsr(newIsr) : null);
            if (written == null) {
                LOG.info("{}: skipped ISR update, leadership changed in ZooKeeper", tp);
                return false;
            }
            state = written;
            return true;
        } catch (KeeperException | RuntimeException e) {
            LOG.warn("{}: failed to update ISR: {}", tp, e.toString());
            return false;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        }
    }

    // ---------------------------------------------------------------- follower side

    /** Appends data fetched from the leader, if we still follow that leader at that epoch. */
    public synchronized void appendAsFollower(List<Message> messages, long leaderHighWatermark,
                                              int leaderId, int epoch) throws IOException {
        if (leader || state == null || state.leader != leaderId || state.leaderEpoch != epoch) {
            return;
        }
        if (!messages.isEmpty() && !log.appendAsFollower(messages)) {
            return;
        }
        long newHw = Math.min(leaderHighWatermark, log.logEndOffset());
        if (newHw > log.highWatermark()) {
            log.setHighWatermark(newHw);
        }
    }

    /** Our log is longer than the leader's: cut the extra (uncommitted) messages. */
    public synchronized void truncateAsFollower(long leaderLogEndOffset, int leaderId, int epoch) throws IOException {
        if (leader || state == null || state.leader != leaderId || state.leaderEpoch != epoch) {
            return;
        }
        LOG.info("{}: log end offset {} is ahead of leader's {}, truncating", tp, log.logEndOffset(), leaderLogEndOffset);
        log.truncateTo(leaderLogEndOffset);
    }
}
