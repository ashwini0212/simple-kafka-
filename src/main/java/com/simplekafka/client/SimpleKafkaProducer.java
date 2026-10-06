package com.simplekafka.client;

import com.simplekafka.protocol.CreateTopicResponse;
import com.simplekafka.protocol.ErrorCode;
import com.simplekafka.protocol.Message;
import com.simplekafka.protocol.MetadataResponse.PartitionMetadata;
import com.simplekafka.protocol.MetadataResponse.TopicMetadata;
import com.simplekafka.protocol.ProduceRequest;

import java.io.BufferedReader;
import java.io.Closeable;
import java.io.IOException;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Sends messages to a topic.
 *
 * <ul>
 *   <li>Picks a partition: hash of the key if there is one (so equal keys stay
 *       in order on one partition), round-robin otherwise.</li>
 *   <li>Creates the topic on first use if it does not exist (3 partitions,
 *       replication factor up to 3).</li>
 *   <li>Uses acks=all by default: a send returns only once every in-sync
 *       replica has the message.</li>
 * </ul>
 *
 * Command line: reads lines from stdin and sends each one.
 * <pre>
 * java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaProducer &lt;host&gt; &lt;port&gt; &lt;topic&gt;
 * </pre>
 */
public class SimpleKafkaProducer implements Closeable {
    public static final int DEFAULT_PARTITIONS = 3;
    public static final int DEFAULT_REPLICATION_FACTOR = 3;
    private static final int REQUEST_TIMEOUT_MS = 10_000;

    private final SimpleKafkaClient client;
    private final short acks;
    private final AtomicInteger roundRobin = new AtomicInteger();

    public SimpleKafkaProducer(String host, int port) {
        this(host, port, ProduceRequest.ACKS_ALL);
    }

    public SimpleKafkaProducer(String host, int port, short acks) {
        this.client = new SimpleKafkaClient(host, port, "producer");
        this.acks = acks;
    }

    public SimpleKafkaClient client() {
        return client;
    }

    public RecordMetadata send(String topic, String key, String value) throws IOException {
        return send(topic, key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    public RecordMetadata send(String topic, byte[] key, byte[] value) throws IOException {
        int partitions = ensureTopic(topic).partitions.size();
        int partition = key == null
                ? (roundRobin.getAndIncrement() & Integer.MAX_VALUE) % partitions
                : (Arrays.hashCode(key) & Integer.MAX_VALUE) % partitions;
        return send(topic, partition, key, value);
    }

    public RecordMetadata send(String topic, int partition, byte[] key, byte[] value) throws IOException {
        List<Message> batch = Collections.singletonList(new Message(key, value));
        long offset = client.produce(topic, partition, batch, acks, REQUEST_TIMEOUT_MS);
        return new RecordMetadata(topic, partition, offset);
    }

    /** Returns the topic's metadata, creating the topic first if needed. */
    public TopicMetadata ensureTopic(String topic) throws IOException {
        TopicMetadata t = client.topicMetadata(topic);
        if (t != null) {
            return t;
        }
        short rf = (short) Math.min(DEFAULT_REPLICATION_FACTOR, client.brokers().size());
        CreateTopicResponse response = client.createTopic(topic, DEFAULT_PARTITIONS, rf);
        if (response.errorCode() != ErrorCode.NONE && response.errorCode() != ErrorCode.TOPIC_ALREADY_EXISTS) {
            throw new SimpleKafkaException(response.errorCode(), response.message());
        }
        try {
            return client.awaitTopicReady(topic, 15_000);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IOException("Interrupted while waiting for topic " + topic, e);
        }
    }

    @Override
    public void close() {
        client.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 3) {
            System.err.println("Usage: SimpleKafkaProducer <host> <port> <topic>");
            System.exit(1);
        }
        String topic = args[2];
        try (SimpleKafkaProducer producer = new SimpleKafkaProducer(args[0], Integer.parseInt(args[1]))) {
            TopicMetadata metadata = producer.ensureTopic(topic);
            System.out.println("Topic " + topic + ":");
            for (PartitionMetadata p : metadata.partitions) {
                System.out.println("  " + p);
            }
            boolean interactive = System.console() != null;
            System.out.println("Type a message and press Enter to send it (Ctrl+D to quit).");
            BufferedReader in = new BufferedReader(new InputStreamReader(System.in, StandardCharsets.UTF_8));
            while (true) {
                if (interactive) {
                    System.out.print("> ");
                    System.out.flush();
                }
                String line = in.readLine();
                if (line == null) {
                    break;
                }
                if (line.isEmpty()) {
                    continue;
                }
                try {
                    RecordMetadata md = producer.send(topic, null, line);
                    System.out.println("Sent to partition " + md.partition() + " at offset " + md.offset());
                } catch (IOException | SimpleKafkaException e) {
                    System.out.println("Send failed: " + e.getMessage());
                }
            }
        }
    }
}
