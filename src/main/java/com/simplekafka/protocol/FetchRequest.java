package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * Read messages from one partition starting at an offset. Sent by consumers
 * (replicaId = -1) and by follower brokers replicating from the leader
 * (replicaId = their broker id).
 *
 * <pre>
 * replicaId int32 | topic string | partition int32 | fetchOffset int64 | maxBytes int32 | maxWaitMs int32
 * </pre>
 *
 * Consumers only see messages below the high watermark (committed messages);
 * followers can read all the way to the log end. A follower's fetchOffset also
 * tells the leader how far that follower has replicated.
 */
public final class FetchRequest implements Request {
    public static final int CONSUMER_REPLICA_ID = -1;

    private final int replicaId;
    private final String topic;
    private final int partition;
    private final long fetchOffset;
    private final int maxBytes;
    private final int maxWaitMs;

    public FetchRequest(int replicaId, String topic, int partition, long fetchOffset, int maxBytes, int maxWaitMs) {
        this.replicaId = replicaId;
        this.topic = topic;
        this.partition = partition;
        this.fetchOffset = fetchOffset;
        this.maxBytes = maxBytes;
        this.maxWaitMs = maxWaitMs;
    }

    @Override
    public short apiKey() {
        return ApiKeys.FETCH;
    }

    public int replicaId() {
        return replicaId;
    }

    public String topic() {
        return topic;
    }

    public int partition() {
        return partition;
    }

    public long fetchOffset() {
        return fetchOffset;
    }

    public int maxBytes() {
        return maxBytes;
    }

    public int maxWaitMs() {
        return maxWaitMs;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(replicaId);
        Protocol.writeString(out, topic);
        out.writeInt(partition);
        out.writeLong(fetchOffset);
        out.writeInt(maxBytes);
        out.writeInt(maxWaitMs);
    }

    public static FetchRequest readFrom(ByteBuffer buf) {
        int replicaId = buf.getInt();
        String topic = Protocol.readString(buf);
        int partition = buf.getInt();
        long fetchOffset = buf.getLong();
        int maxBytes = buf.getInt();
        int maxWaitMs = buf.getInt();
        return new FetchRequest(replicaId, topic, partition, fetchOffset, maxBytes, maxWaitMs);
    }
}
