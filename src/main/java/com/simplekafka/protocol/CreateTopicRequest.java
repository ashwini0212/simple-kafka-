package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Create a topic. Any broker accepts it; non-controller brokers forward it to
 * the controller, which decides the partition assignment.
 *
 * <pre>
 * topic string | numPartitions int32 | replicationFactor int16
 * </pre>
 */
public final class CreateTopicRequest implements Request {
    private final String topic;
    private final int numPartitions;
    private final short replicationFactor;

    public CreateTopicRequest(String topic, int numPartitions, short replicationFactor) {
        this.topic = topic;
        this.numPartitions = numPartitions;
        this.replicationFactor = replicationFactor;
    }

    @Override
    public short apiKey() {
        return ApiKeys.CREATE_TOPIC;
    }

    public String topic() {
        return topic;
    }

    public int numPartitions() {
        return numPartitions;
    }

    public short replicationFactor() {
        return replicationFactor;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        Protocol.writeString(out, topic);
        out.writeInt(numPartitions);
        out.writeShort(replicationFactor);
    }

    public static CreateTopicRequest readFrom(ByteBuffer buf) {
        return new CreateTopicRequest(Protocol.readString(buf), buf.getInt(), buf.getShort());
    }
}
