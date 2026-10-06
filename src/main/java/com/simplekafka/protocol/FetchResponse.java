package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * <pre>
 * errorCode int16 | highWatermark int64 | logEndOffset int64 | count int32 | count * Message
 * </pre>
 */
public final class FetchResponse implements Writable {
    private final short errorCode;
    private final long highWatermark;
    private final long logEndOffset;
    private final List<Message> messages;

    public FetchResponse(short errorCode, long highWatermark, long logEndOffset, List<Message> messages) {
        this.errorCode = errorCode;
        this.highWatermark = highWatermark;
        this.logEndOffset = logEndOffset;
        this.messages = messages;
    }

    public static FetchResponse error(short errorCode) {
        return new FetchResponse(errorCode, -1L, -1L, Collections.emptyList());
    }

    public short errorCode() {
        return errorCode;
    }

    public long highWatermark() {
        return highWatermark;
    }

    public long logEndOffset() {
        return logEndOffset;
    }

    public List<Message> messages() {
        return messages;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(errorCode);
        out.writeLong(highWatermark);
        out.writeLong(logEndOffset);
        out.writeInt(messages.size());
        for (Message m : messages) {
            m.writeTo(out);
        }
    }

    public static FetchResponse readFrom(ByteBuffer buf) {
        short errorCode = buf.getShort();
        long highWatermark = buf.getLong();
        long logEndOffset = buf.getLong();
        int count = buf.getInt();
        List<Message> messages = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            messages.add(Message.readFrom(buf));
        }
        return new FetchResponse(errorCode, highWatermark, logEndOffset, messages);
    }
}
