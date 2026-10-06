package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

/**
 * Ask any broker which brokers exist and who leads each partition. This is how
 * clients find the right broker to talk to.
 *
 * <pre>
 * count int32 | count * topic string      (count 0 = all topics)
 * </pre>
 */
public final class MetadataRequest implements Request {
    private final List<String> topics;

    public MetadataRequest(List<String> topics) {
        this.topics = topics;
    }

    @Override
    public short apiKey() {
        return ApiKeys.METADATA;
    }

    public List<String> topics() {
        return topics;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeInt(topics.size());
        for (String t : topics) {
            Protocol.writeString(out, t);
        }
    }

    public static MetadataRequest readFrom(ByteBuffer buf) {
        int count = buf.getInt();
        List<String> topics = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            topics.add(Protocol.readString(buf));
        }
        return new MetadataRequest(topics);
    }
}
