package com.simplekafka.storage;

import com.simplekafka.protocol.Message;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentSkipListMap;

/**
 * The log of one partition: an ordered list of segments in a directory such as
 * {@code /tmp/simplekafka/broker-1/test-topic-0/}.
 *
 * Messages are only ever appended at the end, and each gets the next offset.
 * When the active (last) segment grows past {@code segmentBytes} a new segment
 * is rolled, named after the next offset.
 *
 * The log also remembers its high watermark: the offset up to which messages
 * are known to be on every in-sync replica. It is checkpointed to a small file
 * so a restarted broker knows how much of its log was committed.
 */
public class Log implements Closeable {
    private static final String HW_CHECKPOINT = "high-watermark-checkpoint";

    private final File dir;
    private final int segmentBytes;
    private final int indexIntervalBytes;
    private final ConcurrentSkipListMap<Long, LogSegment> segments = new ConcurrentSkipListMap<>();
    private volatile long highWatermark;
    private long checkpointedHighWatermark = -1;

    public Log(File dir, int segmentBytes, int indexIntervalBytes) throws IOException {
        this.dir = dir;
        this.segmentBytes = segmentBytes;
        this.indexIntervalBytes = indexIntervalBytes;
        if (!dir.isDirectory() && !dir.mkdirs()) {
            throw new IOException("Could not create " + dir);
        }
        loadSegments();
        highWatermark = Math.min(readHighWatermarkCheckpoint(), logEndOffset());
    }

    private void loadSegments() throws IOException {
        File[] files = dir.listFiles((d, name) -> name.endsWith(".log"));
        List<Long> baseOffsets = new ArrayList<>();
        if (files != null) {
            for (File f : files) {
                baseOffsets.add(Long.parseLong(f.getName().substring(0, f.getName().length() - 4)));
            }
        }
        Collections.sort(baseOffsets);
        for (long base : baseOffsets) {
            segments.put(base, new LogSegment(dir, base, indexIntervalBytes));
        }
        if (segments.isEmpty()) {
            segments.put(0L, new LogSegment(dir, 0L, indexIntervalBytes));
        }
    }

    private LogSegment activeSegment() {
        return segments.lastEntry().getValue();
    }

    private void maybeRoll() throws IOException {
        LogSegment active = activeSegment();
        if (active.size() >= segmentBytes) {
            long base = active.nextOffset();
            active.flush();
            segments.put(base, new LogSegment(dir, base, indexIntervalBytes));
        }
    }

    /** Leader path: assigns offsets and appends. Returns the offset of the first message. */
    public synchronized long append(List<Message> messages) throws IOException {
        long baseOffset = logEndOffset();
        long offset = baseOffset;
        for (Message m : messages) {
            maybeRoll();
            activeSegment().append(m.withOffset(offset++));
        }
        return baseOffset;
    }

    /**
     * Follower path: appends messages that already have offsets assigned by the
     * leader. Messages we already have are skipped; a gap means the data is
     * stale (e.g. we were just truncated) and the whole batch is rejected.
     */
    public synchronized boolean appendAsFollower(List<Message> messages) throws IOException {
        for (Message m : messages) {
            long leo = logEndOffset();
            if (m.offset() < leo) {
                continue;
            }
            if (m.offset() > leo) {
                return false;
            }
            maybeRoll();
            activeSegment().append(m);
        }
        return true;
    }

    /** Reads messages starting at offset, never returning anything at or beyond maxOffset. */
    public List<Message> read(long offset, int maxBytes, long maxOffset) throws IOException {
        Map.Entry<Long, LogSegment> entry = segments.floorEntry(offset);
        while (entry != null) {
            List<Message> messages = entry.getValue().read(offset, maxBytes, maxOffset);
            if (!messages.isEmpty()) {
                return messages;
            }
            entry = segments.higherEntry(entry.getKey());
        }
        return Collections.emptyList();
    }

    /**
     * Removes every message with offset &gt;= offset. A follower does this when
     * it starts following a new leader, to throw away messages the old leader
     * wrote but never committed.
     */
    public synchronized void truncateTo(long offset) throws IOException {
        if (offset >= logEndOffset()) {
            return;
        }
        long firstBase = segments.firstKey();
        for (Long base : new ArrayList<>(segments.tailMap(offset, true).descendingKeySet())) {
            if (base != firstBase) {
                segments.remove(base).delete();
            }
        }
        activeSegment().truncateTo(offset);
        if (highWatermark > offset) {
            highWatermark = offset;
        }
    }

    public long logStartOffset() {
        return segments.firstKey();
    }

    /** The offset the next appended message will get. */
    public long logEndOffset() {
        return activeSegment().nextOffset();
    }

    public long highWatermark() {
        return highWatermark;
    }

    public void setHighWatermark(long hw) {
        this.highWatermark = Math.min(hw, logEndOffset());
    }

    public int segmentCount() {
        return segments.size();
    }

    public File dir() {
        return dir;
    }

    private long readHighWatermarkCheckpoint() {
        File f = new File(dir, HW_CHECKPOINT);
        try {
            return Long.parseLong(new String(Files.readAllBytes(f.toPath()), StandardCharsets.UTF_8).trim());
        } catch (IOException | NumberFormatException e) {
            return 0L;
        }
    }

    /** Writes the high watermark to disk (atomically, via a temp file) if it changed. */
    public synchronized void checkpointHighWatermark() throws IOException {
        long hw = highWatermark;
        if (hw == checkpointedHighWatermark) {
            return;
        }
        File tmp = new File(dir, HW_CHECKPOINT + ".tmp");
        Files.write(tmp.toPath(), Long.toString(hw).getBytes(StandardCharsets.UTF_8));
        Files.move(tmp.toPath(), new File(dir, HW_CHECKPOINT).toPath(),
                StandardCopyOption.REPLACE_EXISTING, StandardCopyOption.ATOMIC_MOVE);
        checkpointedHighWatermark = hw;
    }

    public synchronized void flush() throws IOException {
        for (LogSegment s : segments.values()) {
            s.flush();
        }
    }

    @Override
    public synchronized void close() throws IOException {
        checkpointHighWatermark();
        for (LogSegment s : segments.values()) {
            s.flush();
            s.close();
        }
    }
}
