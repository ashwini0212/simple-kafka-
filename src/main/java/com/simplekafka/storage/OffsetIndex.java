package com.simplekafka.storage;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;

/**
 * A sparse index from message offset to byte position in a segment's .log file.
 *
 * <pre>
 * entry = relativeOffset int32 | position int32     (relativeOffset = offset - segment base offset)
 * </pre>
 *
 * Only one entry is written per {@code indexIntervalBytes} of log data, so the
 * index stays small enough to keep in memory. To find an offset we binary search
 * for the closest entry at or before it, then scan the log forward from there.
 */
public class OffsetIndex implements Closeable {
    static final int ENTRY_SIZE = 8;

    private final File file;
    private final long baseOffset;
    private final FileChannel channel;
    private int[] relativeOffsets = new int[64];
    private int[] positions = new int[64];
    private int entries;

    public OffsetIndex(File file, long baseOffset) throws IOException {
        this.file = file;
        this.baseOffset = baseOffset;
        this.channel = FileChannel.open(file.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        load();
    }

    private void load() throws IOException {
        int count = (int) (channel.size() / ENTRY_SIZE);
        ByteBuffer buf = ByteBuffer.allocate(count * ENTRY_SIZE);
        channel.read(buf, 0);
        buf.flip();
        for (int i = 0; i < count; i++) {
            int rel = buf.getInt();
            int pos = buf.getInt();
            if (entries > 0 && rel <= relativeOffsets[entries - 1]) {
                break; // not monotonic: the rest is garbage from a crash
            }
            add(rel, pos);
        }
        channel.truncate((long) entries * ENTRY_SIZE);
    }

    private void add(int rel, int pos) {
        if (entries == relativeOffsets.length) {
            relativeOffsets = Arrays.copyOf(relativeOffsets, entries * 2);
            positions = Arrays.copyOf(positions, entries * 2);
        }
        relativeOffsets[entries] = rel;
        positions[entries] = pos;
        entries++;
    }

    public synchronized void append(long offset, int position) throws IOException {
        int rel = (int) (offset - baseOffset);
        if (entries > 0 && rel <= relativeOffsets[entries - 1]) {
            return;
        }
        ByteBuffer buf = ByteBuffer.allocate(ENTRY_SIZE);
        buf.putInt(rel).putInt(position).flip();
        channel.write(buf, (long) entries * ENTRY_SIZE);
        add(rel, position);
    }

    /** Byte position of the last indexed message whose offset is &lt;= target, or 0. */
    public synchronized int lookup(long targetOffset) {
        long rel = targetOffset - baseOffset;
        int lo = 0;
        int hi = entries - 1;
        int found = -1;
        while (lo <= hi) {
            int mid = (lo + hi) >>> 1;
            if (relativeOffsets[mid] <= rel) {
                found = mid;
                lo = mid + 1;
            } else {
                hi = mid - 1;
            }
        }
        return found < 0 ? 0 : positions[found];
    }

    /** Position of the last entry, used to resume recovery from there. */
    public synchronized int lastPosition() {
        return entries == 0 ? 0 : positions[entries - 1];
    }

    /** Drops every entry for offsets &gt;= offset. */
    public synchronized void truncateTo(long offset) throws IOException {
        long rel = offset - baseOffset;
        while (entries > 0 && relativeOffsets[entries - 1] >= rel) {
            entries--;
        }
        channel.truncate((long) entries * ENTRY_SIZE);
    }

    /** Drops entries pointing at or beyond the given byte position. */
    public synchronized void truncateToPosition(int position) throws IOException {
        while (entries > 0 && positions[entries - 1] >= position) {
            entries--;
        }
        channel.truncate((long) entries * ENTRY_SIZE);
    }

    public synchronized int entries() {
        return entries;
    }

    public void flush() throws IOException {
        channel.force(true);
    }

    @Override
    public void close() throws IOException {
        channel.close();
    }

    public void delete() throws IOException {
        close();
        if (!file.delete() && file.exists()) {
            throw new IOException("Could not delete " + file);
        }
    }
}
