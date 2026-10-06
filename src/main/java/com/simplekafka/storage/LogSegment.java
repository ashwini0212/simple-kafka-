package com.simplekafka.storage;

import com.simplekafka.protocol.Message;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32;

/**
 * One chunk of a partition's log: an append-only .log file plus its .index file.
 * Files are named after the first offset they hold, zero padded so they sort,
 * e.g. {@code 00000000000000000000.log}.
 *
 * <pre>
 * record = offset int64 | size int32 | crc int32 | timestamp int64 | key bytes | value bytes
 *          \___ LOG_OVERHEAD ___/       \______________ size bytes ______________/
 * </pre>
 *
 * The CRC covers everything after itself, so a record half-written during a
 * crash is detected and cut off on restart.
 */
public class LogSegment implements Closeable {
    public static final int LOG_OVERHEAD = 12;
    private static final int MIN_RECORD_SIZE = 4 + 8 + 4 + 4;

    private final long baseOffset;
    private final File logFile;
    private final FileChannel channel;
    private final OffsetIndex index;
    private final int indexIntervalBytes;
    private volatile int size;
    private volatile long nextOffset;
    private int bytesSinceLastIndexEntry;

    public LogSegment(File dir, long baseOffset, int indexIntervalBytes) throws IOException {
        this.baseOffset = baseOffset;
        this.indexIntervalBytes = indexIntervalBytes;
        this.logFile = new File(dir, fileName(baseOffset, ".log"));
        this.channel = FileChannel.open(logFile.toPath(),
                StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
        this.index = new OffsetIndex(new File(dir, fileName(baseOffset, ".index")), baseOffset);
        this.size = (int) channel.size();
        this.nextOffset = baseOffset;
        recover();
    }

    public static String fileName(long offset, String suffix) {
        return String.format("%020d%s", offset, suffix);
    }

    /**
     * Validates the records after the last index entry (everything before it was
     * already indexed, so it was fully written), rebuilding index entries and
     * truncating a torn or corrupt tail.
     */
    private void recover() throws IOException {
        int position = index.lastPosition();
        if (position > size) {
            index.truncateToPosition(0);
            position = 0;
        }
        bytesSinceLastIndexEntry = 0;
        long lastOffset = baseOffset - 1;
        ByteBuffer header = ByteBuffer.allocate(LOG_OVERHEAD);
        while (position + LOG_OVERHEAD <= size) {
            header.clear();
            channel.read(header, position);
            header.flip();
            long offset = header.getLong();
            int recordSize = header.getInt();
            if (recordSize < MIN_RECORD_SIZE || position + LOG_OVERHEAD + recordSize > size || offset <= lastOffset) {
                break;
            }
            ByteBuffer record = ByteBuffer.allocate(recordSize);
            channel.read(record, position + LOG_OVERHEAD);
            record.flip();
            if (!crcMatches(record)) {
                break;
            }
            if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
                index.append(offset, position);
                bytesSinceLastIndexEntry = 0;
            }
            lastOffset = offset;
            int total = LOG_OVERHEAD + recordSize;
            position += total;
            bytesSinceLastIndexEntry += total;
        }
        if (position < size) {
            channel.truncate(position);
            size = position;
        }
        nextOffset = lastOffset + 1;
    }

    private static boolean crcMatches(ByteBuffer record) {
        int stored = record.getInt(0);
        CRC32 crc = new CRC32();
        crc.update(record.array(), 4, record.limit() - 4);
        return (int) crc.getValue() == stored;
    }

    static ByteBuffer encode(Message m) {
        int keyLength = m.key() == null ? -1 : m.key().length;
        int valueLength = m.value() == null ? -1 : m.value().length;
        int recordSize = MIN_RECORD_SIZE + Math.max(keyLength, 0) + Math.max(valueLength, 0);
        ByteBuffer buf = ByteBuffer.allocate(LOG_OVERHEAD + recordSize);
        buf.putLong(m.offset());
        buf.putInt(recordSize);
        buf.putInt(0); // crc placeholder
        buf.putLong(m.timestamp());
        buf.putInt(keyLength);
        if (m.key() != null) {
            buf.put(m.key());
        }
        buf.putInt(valueLength);
        if (m.value() != null) {
            buf.put(m.value());
        }
        CRC32 crc = new CRC32();
        crc.update(buf.array(), LOG_OVERHEAD + 4, recordSize - 4);
        buf.putInt(LOG_OVERHEAD, (int) crc.getValue());
        buf.flip();
        return buf;
    }

    private static Message decode(long offset, ByteBuffer record) {
        record.getInt(); // crc, already validated on recovery
        long timestamp = record.getLong();
        int keyLength = record.getInt();
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            record.get(key);
        }
        int valueLength = record.getInt();
        byte[] value = null;
        if (valueLength >= 0) {
            value = new byte[valueLength];
            record.get(value);
        }
        return new Message(offset, timestamp, key, value);
    }

    /** Appends a message that already carries its offset. */
    public synchronized void append(Message message) throws IOException {
        if (message.offset() < nextOffset) {
            throw new IllegalArgumentException("Offset " + message.offset() + " < next offset " + nextOffset);
        }
        ByteBuffer buf = encode(message);
        int position = size;
        if (bytesSinceLastIndexEntry >= indexIntervalBytes) {
            index.append(message.offset(), position);
            bytesSinceLastIndexEntry = 0;
        }
        while (buf.hasRemaining()) {
            channel.write(buf, position + buf.position());
        }
        int written = buf.limit();
        size = position + written;
        bytesSinceLastIndexEntry += written;
        nextOffset = message.offset() + 1;
    }

    /** Byte position of the first record with offset &gt;= target (or the end of the file). */
    private int translateOffset(long targetOffset) throws IOException {
        int position = index.lookup(targetOffset);
        int end = size;
        ByteBuffer header = ByteBuffer.allocate(LOG_OVERHEAD);
        while (position + LOG_OVERHEAD <= end) {
            header.clear();
            channel.read(header, position);
            header.flip();
            long offset = header.getLong();
            int recordSize = header.getInt();
            if (offset >= targetOffset) {
                return position;
            }
            position += LOG_OVERHEAD + recordSize;
        }
        return end;
    }

    /**
     * Reads messages with offsets in [startOffset, maxOffset), stopping once
     * maxBytes have been read (but always returning at least one message if any exist).
     */
    public List<Message> read(long startOffset, int maxBytes, long maxOffset) throws IOException {
        List<Message> result = new ArrayList<>();
        int position = translateOffset(startOffset);
        int end = size;
        int bytesRead = 0;
        ByteBuffer header = ByteBuffer.allocate(LOG_OVERHEAD);
        while (position + LOG_OVERHEAD <= end) {
            header.clear();
            channel.read(header, position);
            header.flip();
            long offset = header.getLong();
            int recordSize = header.getInt();
            if (offset >= maxOffset) {
                break;
            }
            if (!result.isEmpty() && bytesRead + LOG_OVERHEAD + recordSize > maxBytes) {
                break;
            }
            ByteBuffer record = ByteBuffer.allocate(recordSize);
            channel.read(record, position + LOG_OVERHEAD);
            record.flip();
            result.add(decode(offset, record));
            bytesRead += LOG_OVERHEAD + recordSize;
            position += LOG_OVERHEAD + recordSize;
        }
        return result;
    }

    /** Removes every message with offset &gt;= offset. */
    public synchronized void truncateTo(long offset) throws IOException {
        if (offset >= nextOffset) {
            return;
        }
        int position = translateOffset(offset);
        channel.truncate(position);
        size = position;
        index.truncateTo(offset);
        nextOffset = Math.max(offset, baseOffset);
        bytesSinceLastIndexEntry = 0;
    }

    public long baseOffset() {
        return baseOffset;
    }

    public long nextOffset() {
        return nextOffset;
    }

    public int size() {
        return size;
    }

    public int indexEntries() {
        return index.entries();
    }

    public void flush() throws IOException {
        channel.force(true);
        index.flush();
    }

    @Override
    public void close() throws IOException {
        channel.close();
        index.close();
    }

    public void delete() throws IOException {
        close();
        index.delete();
        if (!logFile.delete() && logFile.exists()) {
            throw new IOException("Could not delete " + logFile);
        }
    }
}
