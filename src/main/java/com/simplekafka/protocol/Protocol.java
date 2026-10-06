package com.simplekafka.protocol;

import java.io.DataOutputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

/**
 * Primitive encodings shared by every request and response.
 *
 * <pre>
 * string  = int16 length (-1 for null) + UTF-8 bytes
 * bytes   = int32 length (-1 for null) + raw bytes
 * int[]   = int32 count + count * int32
 * </pre>
 *
 * All numbers are big-endian, which is what both DataOutputStream and
 * ByteBuffer use by default.
 */
public final class Protocol {
    /** Upper bound on a single request or response frame, to reject garbage early. */
    public static final int MAX_FRAME_BYTES = 64 * 1024 * 1024;

    private Protocol() {
    }

    public static void writeString(DataOutputStream out, String s) throws IOException {
        if (s == null) {
            out.writeShort(-1);
            return;
        }
        byte[] bytes = s.getBytes(StandardCharsets.UTF_8);
        out.writeShort(bytes.length);
        out.write(bytes);
    }

    public static String readString(ByteBuffer buf) {
        short length = buf.getShort();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return new String(bytes, StandardCharsets.UTF_8);
    }

    public static void writeBytes(DataOutputStream out, byte[] bytes) throws IOException {
        if (bytes == null) {
            out.writeInt(-1);
            return;
        }
        out.writeInt(bytes.length);
        out.write(bytes);
    }

    public static byte[] readBytes(ByteBuffer buf) {
        int length = buf.getInt();
        if (length < 0) {
            return null;
        }
        byte[] bytes = new byte[length];
        buf.get(bytes);
        return bytes;
    }

    public static void writeIntList(DataOutputStream out, List<Integer> values) throws IOException {
        out.writeInt(values.size());
        for (int v : values) {
            out.writeInt(v);
        }
    }

    public static List<Integer> readIntList(ByteBuffer buf) {
        int count = buf.getInt();
        List<Integer> values = new ArrayList<>(count);
        for (int i = 0; i < count; i++) {
            values.add(buf.getInt());
        }
        return values;
    }
}
