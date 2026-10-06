package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <pre>
 * errorCode int16 | baseOffset int64
 * </pre>
 * baseOffset is the offset given to the first message of the request.
 */
public final class ProduceResponse implements Writable {
    private final short errorCode;
    private final long baseOffset;

    public ProduceResponse(short errorCode, long baseOffset) {
        this.errorCode = errorCode;
        this.baseOffset = baseOffset;
    }

    public short errorCode() {
        return errorCode;
    }

    public long baseOffset() {
        return baseOffset;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(errorCode);
        out.writeLong(baseOffset);
    }

    public static ProduceResponse readFrom(ByteBuffer buf) {
        return new ProduceResponse(buf.getShort(), buf.getLong());
    }
}
