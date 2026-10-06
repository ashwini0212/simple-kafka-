package com.simplekafka.client;

import com.simplekafka.protocol.BrokerInfo;
import com.simplekafka.protocol.CreateTopicRequest;
import com.simplekafka.protocol.CreateTopicResponse;
import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.FetchRequest;
import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.KafkaConnection;
import com.simplekafka.protocol.Message;
import com.simplekafka.protocol.MetadataRequest;
import com.simplekafka.protocol.MetadataResponse;
import com.simplekafka.protocol.MetadataResponse.PartitionMetadata;
import com.simplekafka.protocol.MetadataResponse.TopicMetadata;
import com.simplekafka.protocol.ProduceRequest;
import com.simplekafka.protocol.ProduceResponse;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * The low-level client shared by the producer and consumer.
 *
 * It starts from a single "bootstrap" broker, asks it for cluster metadata,
 * and from then on sends each request straight to the leader of the partition
 * involved. When a leader moves (NOT_LEADER_FOR_PARTITION, or the connection
 * drops) it refreshes metadata and retries.
 */
public class SimpleKafkaClient implements Closeable {
    private static final Logger LOG = LoggerFactory.getLogger(SimpleKafkaClient.class);

    private final String bootstrapHost;
    private final int bootstrapPort;
    private final String clientId;
    private final Map<Integer, BrokerInfo> brokers = new ConcurrentHashMap<>();
    private final Map<String, TopicMetadata> topics = new ConcurrentHashMap<>();
    private final Map<Integer, KafkaConnection> connections = new ConcurrentHashMap<>();
    private int maxRetries = 20;
    private long retryBackoffMs = 500;

    public SimpleKafkaClient(String bootstrapHost, int bootstrapPort, String clientId) {
        this.bootstrapHost = bootstrapHost;
        this.bootstrapPort = bootstrapPort;
        this.clientId = clientId;
    }

    public void setRetries(int maxRetries, long retryBackoffMs) {
        this.maxRetries = maxRetries;
        this.retryBackoffMs = retryBackoffMs;
    }

    // ------------------------------------------------------------------ metadata

    /** Fetches fresh metadata from the bootstrap broker, falling back to any broker we know. */
    public synchronized MetadataResponse refreshMetadata(List<String> topicNames) throws IOException {
        List<String[]> candidates = new ArrayList<>();
        candidates.add(new String[]{bootstrapHost, Integer.toString(bootstrapPort)});
        for (BrokerInfo b : brokers.values()) {
            candidates.add(new String[]{b.host(), Integer.toString(b.port())});
        }
        IOException last = null;
        for (String[] c : candidates) {
            try (KafkaConnection connection = new KafkaConnection(c[0], Integer.parseInt(c[1]), clientId)) {
                MetadataResponse response = MetadataResponse.readFrom(connection.send(new MetadataRequest(topicNames)));
                brokers.clear();
                for (BrokerInfo b : response.brokers()) {
                    brokers.put(b.id(), b);
                }
                for (TopicMetadata t : response.topics()) {
                    if (t.errorCode == ErrorCode.NONE) {
                        topics.put(t.name, t);
                    } else {
                        topics.remove(t.name);
                    }
                }
                return response;
            } catch (IOException e) {
                last = e;
            }
        }
        throw new IOException("No broker reachable for metadata: " + last, last);
    }

    /** Cached metadata for a topic, fetched if we have none. Null if the topic does not exist. */
    public TopicMetadata topicMetadata(String topic) throws IOException {
        TopicMetadata t = topics.get(topic);
        if (t == null) {
            refreshMetadata(Collections.singletonList(topic));
            t = topics.get(topic);
        }
        return t;
    }

    public List<BrokerInfo> brokers() throws IOException {
        if (brokers.isEmpty()) {
            refreshMetadata(Collections.emptyList());
        }
        return new ArrayList<>(brokers.values());
    }

    public CreateTopicResponse createTopic(String topic, int partitions, short replicationFactor) throws IOException {
        try (KafkaConnection connection = new KafkaConnection(bootstrapHost, bootstrapPort, clientId)) {
            return CreateTopicResponse.readFrom(
                    connection.send(new CreateTopicRequest(topic, partitions, replicationFactor)));
        }
    }

    /** Waits until every partition of the topic has a leader. */
    public TopicMetadata awaitTopicReady(String topic, long timeoutMs) throws IOException, InterruptedException {
        long deadline = System.currentTimeMillis() + timeoutMs;
        while (true) {
            refreshMetadata(Collections.singletonList(topic));
            TopicMetadata t = topics.get(topic);
            if (t != null && !t.partitions.isEmpty() && t.partitions.stream().allMatch(p -> p.leader >= 0)) {
                return t;
            }
            if (System.currentTimeMillis() > deadline) {
                throw new IOException("Topic " + topic + " has no leader for every partition after " + timeoutMs + " ms");
            }
            Thread.sleep(200);
        }
    }

    // ------------------------------------------------------------------ produce / fetch

    /** Sends messages to the partition leader and returns the offset of the first one. */
    public long produce(String topic, int partition, List<Message> messages, short acks, int timeoutMs)
            throws IOException {
        ProduceRequest request = new ProduceRequest(topic, partition, acks, timeoutMs, messages);
        return withRetries(topic, partition, connection -> {
            ProduceResponse response = ProduceResponse.readFrom(connection.send(request));
            if (response.errorCode() != ErrorCode.NONE) {
                throw new SimpleKafkaException(response.errorCode(), "produce to " + topic + "-" + partition);
            }
            return response.baseOffset();
        });
    }

    /** Fetches messages from the partition leader, starting at offset. */
    public FetchResponse fetch(String topic, int partition, long offset, int maxBytes, int maxWaitMs)
            throws IOException {
        FetchRequest request = new FetchRequest(FetchRequest.CONSUMER_REPLICA_ID, topic, partition, offset,
                maxBytes, maxWaitMs);
        return withRetries(topic, partition, connection -> {
            FetchResponse response = FetchResponse.readFrom(connection.send(request));
            if (response.errorCode() != ErrorCode.NONE) {
                throw new SimpleKafkaException(response.errorCode(), "fetch from " + topic + "-" + partition
                        + " at offset " + offset + " (log end offset " + response.logEndOffset() + ")");
            }
            return response;
        });
    }

    private interface LeaderCall<T> {
        T call(KafkaConnection leader) throws IOException;
    }

    private <T> T withRetries(String topic, int partition, LeaderCall<T> call) throws IOException {
        RuntimeException lastError = null;
        IOException lastIo = null;
        for (int attempt = 0; attempt <= maxRetries; attempt++) {
            int leader = -1;
            try {
                leader = leaderFor(topic, partition);
                return call.call(connectionTo(leader));
            } catch (SimpleKafkaException e) {
                if (!e.isRetriable()) {
                    throw e;
                }
                lastError = e;
            } catch (IOException e) {
                lastIo = e;
                KafkaConnection dead = connections.remove(leader);
                if (dead != null) {
                    dead.close();
                }
            }
            if (attempt == 0) {
                LOG.info("Leader for {}-{} unavailable ({}), refreshing metadata and retrying", topic, partition,
                        lastError != null ? lastError.getMessage() : lastIo.toString());
            }
            sleep(retryBackoffMs);
            try {
                refreshMetadata(Collections.singletonList(topic));
            } catch (IOException e) {
                lastIo = e;
            }
        }
        if (lastError != null) {
            throw lastError;
        }
        throw lastIo;
    }

    private int leaderFor(String topic, int partition) throws IOException {
        TopicMetadata t = topicMetadata(topic);
        if (t == null) {
            throw new SimpleKafkaException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "topic " + topic + " does not exist");
        }
        PartitionMetadata p = t.partition(partition);
        if (p == null) {
            throw new SimpleKafkaException(ErrorCode.INVALID_PARTITIONS,
                    "topic " + topic + " has no partition " + partition);
        }
        if (p.leader < 0) {
            throw new SimpleKafkaException(ErrorCode.LEADER_NOT_AVAILABLE, topic + "-" + partition + " has no leader");
        }
        return p.leader;
    }

    private KafkaConnection connectionTo(int brokerId) throws IOException {
        KafkaConnection c = connections.get(brokerId);
        if (c == null) {
            BrokerInfo b = brokers.get(brokerId);
            if (b == null) {
                throw new IOException("Unknown broker " + brokerId);
            }
            c = new KafkaConnection(b.host(), b.port(), clientId);
            connections.put(brokerId, c);
        }
        return c;
    }

    private static void sleep(long ms) throws IOException {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted", e);
        }
    }

    @Override
    public void close() {
        for (KafkaConnection c : connections.values()) {
            c.close();
        }
        connections.clear();
    }
}
