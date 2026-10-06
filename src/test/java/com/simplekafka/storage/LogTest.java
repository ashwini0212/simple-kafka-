package com.simplekafka.storage;

import com.simplekafka.protocol.Message;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.io.File;
import java.io.RandomAccessFile;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class LogTest {
    @TempDir
    File dir;

    private static List<Message> messages(int from, int count) {
        List<Message> list = new ArrayList<>();
        for (int i = from; i < from + count; i++) {
            list.add(Message.of("k" + i, "value-" + i));
        }
        return list;
    }

    @Test
    void appendsAssignSequentialOffsetsAndRollSegments() throws Exception {
        try (Log log = new Log(dir, 1024, 128)) {
            assertEquals(0, log.append(messages(0, 50)));
            assertEquals(50, log.append(messages(50, 50)));
            assertEquals(100, log.logEndOffset());
            assertTrue(log.segmentCount() > 1, "small segment size should force rolling");

            List<Message> read = log.read(37, Integer.MAX_VALUE, Long.MAX_VALUE);
            assertEquals(37, read.get(0).offset());
            assertEquals("value-37", read.get(0).valueAsString());
            assertEquals("k37", read.get(0).keyAsString());
        }
    }

    @Test
    void readReturnsEverySingleOffsetInOrder() throws Exception {
        try (Log log = new Log(dir, 2048, 256)) {
            log.append(messages(0, 300));
            long next = 0;
            while (next < 300) {
                List<Message> batch = log.read(next, 500, Long.MAX_VALUE);
                for (Message m : batch) {
                    assertEquals(next, m.offset());
                    assertEquals("value-" + next, m.valueAsString());
                    next++;
                }
            }
            assertTrue(log.read(300, 500, Long.MAX_VALUE).isEmpty());
            assertEquals(10, log.read(0, Integer.MAX_VALUE, 10).size(), "maxOffset caps the read");
        }
    }

    @Test
    void reopenRecoversStateAndCutsTornWrite() throws Exception {
        try (Log log = new Log(dir, 1024, 128)) {
            log.append(messages(0, 40));
            log.setHighWatermark(30);
        }
        // Simulate a crash in the middle of a write: garbage at the end of the active segment.
        File[] logs = dir.listFiles((d, n) -> n.endsWith(".log"));
        java.util.Arrays.sort(logs);
        try (RandomAccessFile f = new RandomAccessFile(logs[logs.length - 1], "rw")) {
            f.seek(f.length());
            f.write(new byte[]{0, 0, 0, 0, 0, 0, 0, 40, 0, 0, 0, 99, 1, 2, 3});
        }
        try (Log log = new Log(dir, 1024, 128)) {
            assertEquals(40, log.logEndOffset());
            assertEquals(30, log.highWatermark());
            assertEquals(40, log.append(Collections.singletonList(Message.of(null, "after-restart"))));
            assertEquals("after-restart", log.read(40, 1000, Long.MAX_VALUE).get(0).valueAsString());
        }
    }

    @Test
    void truncateRemovesTailAcrossSegments() throws Exception {
        try (Log log = new Log(dir, 512, 64)) {
            log.append(messages(0, 100));
            int before = log.segmentCount();
            log.truncateTo(20);
            assertEquals(20, log.logEndOffset());
            assertTrue(log.segmentCount() < before);
            assertEquals(20, log.append(messages(500, 1)));
            assertEquals("value-500", log.read(20, 1000, Long.MAX_VALUE).get(0).valueAsString());
            assertEquals("value-19", log.read(19, 1, Long.MAX_VALUE).get(0).valueAsString());
        }
    }

    @Test
    void followerAppendKeepsLeaderOffsetsAndRejectsGaps() throws Exception {
        try (Log log = new Log(dir, 1024, 128)) {
            List<Message> fromLeader = new ArrayList<>();
            for (int i = 0; i < 5; i++) {
                fromLeader.add(Message.of(null, "m" + i).withOffset(i));
            }
            assertTrue(log.appendAsFollower(fromLeader));
            assertTrue(log.appendAsFollower(fromLeader), "duplicates are skipped");
            assertEquals(5, log.logEndOffset());
            assertEquals(false, log.appendAsFollower(
                    Collections.singletonList(Message.of(null, "gap").withOffset(9))));
        }
    }
}
