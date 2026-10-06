package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;

/**
 * A single message: an optional key, a value, a timestamp, and the offset the
 * broker assigned when it was appended (-1 until then).
 *
 * <pre>
 * wire format: offset int64 | timestamp int64 | key bytes | value bytes
 * </pre>
 */
public final class Message {
    private final long offset;
    private final long timestamp;
    private final byte[] key;
    private final byte[] value;

    public Message(byte[] key, byte[] value) {
        this(-1L, System.currentTimeMillis(), key, value);
    }

    public Message(long offset, long timestamp, byte[] key, byte[] value) {
        this.offset = offset;
        this.timestamp = timestamp;
        this.key = key;
        this.value = value;
    }

    public static Message of(String key, String value) {
        return new Message(key == null ? null : key.getBytes(StandardCharsets.UTF_8),
                value == null ? null : value.getBytes(StandardCharsets.UTF_8));
    }

    public Message withOffset(long newOffset) {
        return new Message(newOffset, timestamp, key, value);
    }

    public long offset() {
        return offset;
    }

    public long timestamp() {
        return timestamp;
    }

    public byte[] key() {
        return key;
    }

    public byte[] value() {
        return value;
    }

    public String keyAsString() {
        return key == null ? null : new String(key, StandardCharsets.UTF_8);
    }

    public String valueAsString() {
        return value == null ? null : new String(value, StandardCharsets.UTF_8);
    }

    public void writeTo(DataOutputStream out) throws IOException {
        out.writeLong(offset);
        out.writeLong(timestamp);
        Protocol.writeBytes(out, key);
        Protocol.writeBytes(out, value);
    }

    public static Message readFrom(ByteBuffer buf) {
        long offset = buf.getLong();
        long timestamp = buf.getLong();
        byte[] key = Protocol.readBytes(buf);
        byte[] value = Protocol.readBytes(buf);
        return new Message(offset, timestamp, key, value);
    }

    @Override
    public String toString() {
        return "Message(offset=" + offset + ", key=" + keyAsString() + ", value=" + valueAsString() + ")";
    }
}
