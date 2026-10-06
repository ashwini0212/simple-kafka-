package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Append messages to one partition. Must be sent to that partition's leader.
 *
 * <pre>
 * topic string | partition int32 | acks int16 | timeoutMs int32 | count int32 | count * Message
 * </pre>
 *
 * acks = 1 means "reply once the leader wrote it"; acks = -1 means "reply once
 * every in-sync replica has it", which is what makes a write survive a leader
 * crash.
 */
public final class ProduceRequest implements Request {
    public static final short ACKS_LEADER = 1;
    public static final short ACKS_ALL = -1;

    private final String topic;
    private final int partition;
    private final short acks;
    private final int timeoutMs;
    private final List<Message> messages;

    public ProduceRequest(String topic, int partition, short acks, int timeoutMs, List<Message> messages) {
        this.topic = topic;
        this.partition = partition;
        this.acks = acks;
        this.timeoutMs = timeoutMs;
        this.messages = messages;
    }

    @Override
    public short apiKey() {
        return ApiKeys.PRODUCE;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public short acks() {
        return acks;
    }

    public int timeoutMs() {
        return timeoutMs;
    }

    public List<Message> messages() {
        return messages;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        Protocol.writeString(out, topic);
        out.writeInt(partition);
        out.writeShort(acks);
        out.writeInt(timeoutMs);
        out.writeInt(messages.size());
        for (Message m : messages) {
            m.writeTo(out);
        }
    }

    public static ProduceRequest readFrom(ByteBuffer buf) {
        String topic = Protocol.readString(buf);
        int partition = buf.getInt();
        short acks = buf.getShort();
        int timeoutMs = buf.getInt();
        int count = buf.getInt();
        List<Message> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(Message.readFrom(buf));
        }
        return new ProduceRequest(topic, partition, acks, timeoutMs, messages);
    }
}
