package com.simplekafka.broker;

import com.simplekafka.protocol.ApiKeys;
import com.simplekafka.protocol.BrokerInfo;
import com.simplekafka.protocol.CreateTopicRequest;
import com.simplekafka.protocol.CreateTopicResponse;
import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.KafkaConnection;
import com.simplekafka.protocol.MetadataRequest;
import com.simplekafka.protocol.ProduceRequest;
import com.simplekafka.protocol.ProduceResponse;
import com.simplekafka.protocol.TopicPartition;
import com.simplekafka.protocol.Writable;
import com.simplekafka.zk.ZkPaths;
import com.simplekafka.zk.ZookeeperClient;
import org.apache.zookeeper.Watcher;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * A SimpleKafka broker.
 *
 * <pre>
 * java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.broker.SimpleKafkaBroker &lt;id&gt; &lt;host&gt; &lt;port&gt; &lt;zkPort|zkHost:zkPort&gt;
 * </pre>
 *
 * On startup it registers itself under /brokers/ids, joins the controller
 * election, and watches topics and partition states in ZooKeeper. All
 * ZooKeeper notifications are handled one at a time on a single event thread,
 * so cluster-state changes never race with each other.
 */
public class SimpleKafkaBroker {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaBroker.class);

    private final BrokerConfig config;
    private final MetadataCache cache = new MetadataCache();
    private final ExecutorService eventThread = Executors.newSingleThreadExecutor(r -> {
        Thread t = new Thread(r, "zk-event-thread");
        t.setDaemon(true);
        return t;
    });
    private final ScheduledExecutorService scheduler = Executors.newSingleThreadScheduledExecutor(r -> {
        Thread t = new Thread(r, "scheduler");
        t.setDaemon(true);
        return t;
    });
    private final CountDownLatch shutdownLatch = new CountDownLatch(1);
    private final AtomicBoolean shuttingDown = new AtomicBoolean();

    private ZookeeperClient zk;
    private ReplicaManager replicaManager;
    private Controller controller;
    private SocketServer socketServer;
    private Watcher controllerWatcher;

    public SimpleKafkaBroker(BrokerConfig config) {
        this.config = config;
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SimpleKafkaBroker <brokerId> <host> <port> <zkPort | zkHost:zkPort>");
            System.exit(1);
        }
        int brokerId = Integer.parseInt(args[0]);
        String host = args[1];
        int port = Integer.parseInt(args[2]);
        String zkConnect = args[3].contains(":") ? args[3] : "localhost:" + args[3];

        SimpleKafkaBroker broker = new SimpleKafkaBroker(new BrokerConfig(brokerId, host, port, zkConnect));
        Runtime.getRuntime().addShutdownHook(new Thread(broker::shutdown, "shutdown-hook"));
        try {
            broker.startup();
        } catch (Exception e) {
            LOG.error("Broker failed to start: {}", e.toString());
            System.exit(1);
        }
        broker.awaitShutdown();
    }

    public void startup() throws Exception {
        LOG.info("Starting broker {} on {}:{} (ZooKeeper {}, data dir {})",
                config.brokerId, config.host, config.port, config.zkConnect, config.dataDir);

        zk = new ZookeeperClient(config.zkConnect, config.zkSessionTimeoutMs, this::onSessionExpired);
        zk.connect(15_000);
        zk.ensurePath(ZkPaths.BROKER_IDS);
        zk.ensurePath(ZkPaths.TOPICS);

        ZkPartitionStates zkStates = new ZkPartitionStates(zk);
        replicaManager = new ReplicaManager(config, cache, zkStates);
        controller = new Controller(config.brokerId, zk, cache, zkStates);
        controllerWatcher = watcher(this::watchController);

        socketServer = new SocketServer(config.port, this::handleRequest);
        socketServer.start();
        registerBroker();

        runOnEventThread(this::loadBrokers);
        runOnEventThread(this::loadTopics);
        runOnEventThread(this::watchController);

        scheduler.scheduleAtFixedRate(() -> replicaManager.maybeShrinkIsrs(), 1, 1, TimeUnit.SECONDS);
        scheduler.scheduleAtFixedRate(() -> replicaManager.checkpointHighWatermarks(), 1, 1, TimeUnit.SECONDS);
        LOG.info("Broker {} started", config.brokerId);
    }

    /**
     * Registers /brokers/ids/&lt;id&gt; as an ephemeral node. If this broker was just
     * killed and restarted, the old registration lingers until its session times
     * out, so we wait for it.
     */
    private void registerBroker() throws Exception {
        BrokerInfo me = new BrokerInfo(config.brokerId, config.host, config.port);
        long deadline = System.currentTimeMillis() + 3L * config.zkSessionTimeoutMs;
        while (!zk.createEphemeral(ZkPaths.broker(config.brokerId), me.toZkData())) {
            if (System.currentTimeMillis() > deadline) {
                throw new IllegalStateException("Broker id " + config.brokerId + " is already registered");
            }
            LOG.info("Waiting for the previous registration of broker {} to expire", config.brokerId);
            Thread.sleep(1000);
        }
        LOG.info("Registered {} in ZooKeeper", me);
    }

    // ------------------------------------------------------------------ ZooKeeper events

    private interface ZkTask {
        void run() throws Exception;
    }

    private void runOnEventThread(ZkTask task) {
        if (shuttingDown.get()) {
            return;
        }
        eventThread.submit(() -> {
            try {
                task.run();
            } catch (Exception e) {
                if (!shuttingDown.get()) {
                    LOG.error("Error handling ZooKeeper event", e);
                }
            }
        });
    }

    /** A one-shot watch that re-runs task on the event thread when the node changes. */
    private Watcher watcher(ZkTask task) {
        return event -> {
            if (event.getType() != Watcher.Event.EventType.None) {
                runOnEventThread(task);
            }
        };
    }

    private void loadBrokers() throws Exception {
        List<String> ids = zk.getChildren(ZkPaths.BROKER_IDS, watcher(this::loadBrokers));
        Map<Integer, BrokerInfo> brokers = new HashMap<>();
        Map<Integer, Long> sessions = new HashMap<>();
        for (String idString : ids) {
            int id = Integer.parseInt(idString);
            Stat stat = new Stat();
            byte[] data = zk.getData(ZkPaths.broker(id), null, stat);
            if (data != null) {
                brokers.put(id, BrokerInfo.fromZkData(id, data));
                sessions.put(id, stat.getCzxid());
            }
        }
        Set<Integer> joined = new TreeSet<>(brokers.keySet());
        joined.removeAll(cache.liveBrokers.keySet());
        Set<Integer> left = new TreeSet<>(cache.liveBrokers.keySet());
        left.removeAll(brokers.keySet());
        Set<Integer> bounced = new HashSet<>();
        for (Map.Entry<Integer, Long> e : sessions.entrySet()) {
            Long previous = cache.brokerSessions.get(e.getKey());
            if (previous != null && !previous.equals(e.getValue())) {
                bounced.add(e.getKey());
            }
        }

        cache.liveBrokers.keySet().retainAll(brokers.keySet());
        cache.liveBrokers.putAll(brokers);
        cache.brokerSessions.keySet().retainAll(sessions.keySet());
        cache.brokerSessions.putAll(sessions);

        if (!joined.isEmpty() || !left.isEmpty() || !bounced.isEmpty()) {
            LOG.info("Live brokers now {} (joined {}, left {}, restarted {})",
                    new TreeSet<>(brokers.keySet()), joined, left, bounced);
            controller.onBrokersChanged(joined, left, bounced);
        }
    }

    private void loadTopics() throws Exception {
        List<String> topics = zk.getChildren(ZkPaths.TOPICS, watcher(this::loadTopics));
        for (String topic : topics) {
            if (cache.assignments.containsKey(topic)) {
                continue;
            }
            byte[] data = zk.getData(ZkPaths.topic(topic), null, null);
            if (data == null) {
                continue;
            }
            Map<Integer, List<Integer>> assignment = TopicAssignment.fromZkData(data);
            cache.assignments.put(topic, assignment);
            LOG.info("Discovered topic {} with replica assignment {}", topic, assignment);
            for (int p : assignment.keySet()) {
                watchPartitionState(new TopicPartition(topic, p));
            }
        }
    }

    private void watchPartitionState(TopicPartition tp) throws Exception {
        Stat stat = new Stat();
        byte[] data = zk.getData(ZkPaths.partitionState(tp.topic(), tp.partition()),
                watcher(() -> watchPartitionState(tp)), stat);
        if (data == null) {
            return;
        }
        PartitionState state = PartitionState.fromZkData(data, stat.getVersion());
        cache.states.put(tp, state);
        replicaManager.onPartitionState(tp, cache.replicas(tp), state);
    }

    private void watchController() throws Exception {
        Stat stat = zk.exists(ZkPaths.CONTROLLER, controllerWatcher);
        if (stat == null) {
            cache.controllerId = -1;
            controller.tryElect();
            return; // our exists() watch fires NodeCreated and brings us back here
        }
        byte[] data = zk.getData(ZkPaths.CONTROLLER, controllerWatcher, null);
        if (data == null) {
            runOnEventThread(this::watchController);
            return;
        }
        int controllerId = Integer.parseInt(new String(data, StandardCharsets.UTF_8).trim());
        if (controllerId != cache.controllerId) {
            LOG.info("Current controller is broker {}", controllerId);
        }
        cache.controllerId = controllerId;
        if (controllerId != config.brokerId) {
            controller.resign();
        }
    }

    private void onSessionExpired() {
        LOG.error("Lost ZooKeeper session; shutting down so the cluster fails over cleanly");
        new Thread(() -> System.exit(1), "exit").start();
    }

    // ------------------------------------------------------------------ requests

    private Writable handleRequest(short apiKey, String clientId, ByteBuffer body) throws Exception {
        switch (apiKey) {
            case ApiKeys.PRODUCE:
                return handleProduce(ProduceRequest.readFrom(body));
            case ApiKeys.FETCH:
                return handleFetch(FetchRequest.readFrom(body));
            case ApiKeys.METADATA:
                return cache.buildMetadata(MetadataRequest.readFrom(body).topics());
            case ApiKeys.CREATE_TOPIC:
                return handleCreateTopic(CreateTopicRequest.readFrom(body));
            default:
                throw new IOException("Unsupported api key " + ApiKeys.name(apiKey) + " from " + clientId);
        }
    }

    private short missingPartitionError(TopicPartition tp) {
        return cache.replicas(tp).isEmpty() ? ErrorCode.UNKNOWN_TOPIC_OR_PARTITION : ErrorCode.NOT_LEADER_FOR_PARTITION;
    }

    private ProduceResponse handleProduce(ProduceRequest request) throws Exception {
        TopicPartition tp = new TopicPartition(request.topic(), request.partition());
        Partition partition = replicaManager.getPartition(tp);
        if (partition == null) {
            return new ProduceResponse(missingPartitionError(tp), -1L);
        }
        return partition.appendAsLeader(request);
    }

    private FetchResponse handleFetch(FetchRequest request) throws Exception {
        TopicPartition tp = new TopicPartition(request.topic(), request.partition());
        Partition partition = replicaManager.getPartition(tp);
        if (partition == null) {
            return FetchResponse.error(missingPartitionError(tp));
        }
        return partition.fetch(request);
    }

    /** Only the controller creates topics; other brokers forward the request to it. */
    private CreateTopicResponse handleCreateTopic(CreateTopicRequest request) throws Exception {
        if (controller.isActive()) {
            return controller.createTopic(request);
        }
        BrokerInfo controllerBroker = cache.liveBrokers.get(cache.controllerId);
        if (controllerBroker == null) {
            return new CreateTopicResponse(ErrorCode.NOT_CONTROLLER, "No controller is available right now");
        }
        try (KafkaConnection connection = new KafkaConnection(controllerBroker.host(), controllerBroker.port(),
                "broker-" + config.brokerId)) {
            return CreateTopicResponse.readFrom(connection.send(request));
        } catch (IOException e) {
            return new CreateTopicResponse(ErrorCode.NOT_CONTROLLER, "Could not reach controller: " + e);
        }
    }

    // ------------------------------------------------------------------ shutdown

    public void shutdown() {
        if (!shuttingDown.compareAndSet(false, true)) {
            return;
        }
        LOG.info("Shutting down broker {}", config.brokerId);
        if (socketServer != null) {
            socketServer.shutdown();
        }
        scheduler.shutdownNow();
        if (replicaManager != null) {
            replicaManager.stopFetchers();
        }
        eventThread.shutdownNow();
        if (zk != null) {
            zk.close(); // deletes our ephemeral nodes, so the controller reacts immediately
        }
        if (replicaManager != null) {
            replicaManager.closeLogs();
        }
        LOG.info("Broker {} shut down", config.brokerId);
        shutdownLatch.countDown();
    }

    public void awaitShutdown() throws InterruptedException {
        shutdownLatch.await();
    }
}
