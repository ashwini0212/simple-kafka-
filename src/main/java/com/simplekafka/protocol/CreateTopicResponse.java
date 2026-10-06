package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;

/**
 * <pre>
 * errorCode int16 | message string
 * </pre>
 */
public final class CreateTopicResponse implements Writable {
    private final short errorCode;
    private final String message;

    public CreateTopicResponse(short errorCode, String message) {
        this.errorCode = errorCode;
        this.message = message;
    }

    public short errorCode() {
        return errorCode;
    }

    public String message() {
        return message;
    }

    @Override
    public void writeTo(DataOutputStream out) throws IOException {
        out.writeShort(errorCode);
        Protocol.writeString(out, message);
    }

    public static CreateTopicResponse readFrom(ByteBuffer buf) {
        return new CreateTopicResponse(buf.getShort(), Protocol.readString(buf));
    }
}
