package com.simplekafka.client;

import com.simplekafka.protocol.FetchResponse;
import com.simplekafka.protocol.Message;

import java.io.Closeable;
import java.io.IOException;
import java.util.List;

/**
 * Reads one partition of a topic, starting from an offset, from that
 * partition's leader. The consumer tracks its own position: after each poll
 * it moves to the offset after the last message it received.
 *
 * Only committed messages (below the high watermark) are returned, so a
 * consumer never sees a message that could later vanish in a leader failover.
 *
 * Command line: prints messages as they arrive.
 * <pre>
 * java -cp target/simple-kafka-1.0-SNAPSHOT.jar com.simplekafka.client.SimpleKafkaConsumer &lt;host&gt; &lt;port&gt; &lt;topic&gt; &lt;partition&gt; [startOffset]
 * </pre>
 */
public class SimpleKafkaConsumer implements Closeable {
    private static final int MAX_FETCH_BYTES = 1024 * 1024;

    private final SimpleKafkaClient client;
    private final String topic;
    private final int partition;
    private long position;

    public SimpleKafkaConsumer(String host, int port, String topic, int partition) {
        this.client = new SimpleKafkaClient(host, port, "consumer");
        this.topic = topic;
        this.partition = partition;
    }

    public void seek(long offset) {
        this.position = offset;
    }

    public long position() {
        return position;
    }

    /** Returns the next messages, waiting up to maxWaitMs on the broker if none are available yet. */
    public List<Message> poll(int maxWaitMs) throws IOException {
        FetchResponse response = client.fetch(topic, partition, position, MAX_FETCH_BYTES, maxWaitMs);
        List<Message> messages = response.messages();
        if (!messages.isEmpty()) {
            position = messages.get(messages.size() - 1).offset() + 1;
        }
        return messages;
    }

    public boolean topicExists() throws IOException {
        client.refreshMetadata(java.util.Collections.singletonList(topic));
        return client.topicMetadata(topic) != null;
    }

    @Override
    public void close() {
        client.close();
    }

    public static void main(String[] args) throws Exception {
        if (args.length < 4) {
            System.err.println("Usage: SimpleKafkaConsumer <host> <port> <topic> <partition> [startOffset]");
            System.exit(1);
        }
        String topic = args[2];
        int partition = Integer.parseInt(args[3]);
        try (SimpleKafkaConsumer consumer = new SimpleKafkaConsumer(args[0], Integer.parseInt(args[1]), topic, partition)) {
            consumer.seek(args.length > 4 ? Long.parseLong(args[4]) : 0L);
            boolean announced = false;
            while (!consumer.topicExists()) {
                if (!announced) {
                    System.out.println("Waiting for topic " + topic + " to be created...");
                    announced = true;
                }
                Thread.sleep(1000);
            }
            System.out.println("Consuming " + topic + "-" + partition + " from offset " + consumer.position()
                    + " (Ctrl+C to quit)");
            while (true) {
                try {
                    for (Message m : consumer.poll(1000)) {
                        String key = m.key() == null ? "" : " key=" + m.keyAsString();
                        System.out.println("offset=" + m.offset() + key + " value=" + m.valueAsString());
                    }
                } catch (IOException | SimpleKafkaException e) {
                    System.out.println("Fetch failed, retrying: " + e.getMessage());
                    Thread.sleep(1000);
                }
            }
        }
    }
}
