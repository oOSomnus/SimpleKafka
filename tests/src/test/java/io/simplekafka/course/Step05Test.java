package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.IndexEntry;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.storage.SparseIndex;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step05Test {
    @Test
    void writesBigEndianEntriesAtIntervalAndFloorsSafely() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SparseIndex index = new SparseIndex(temp.root().resolve("segment.index"), 0, 4)) {
            for (int offset = 0; offset <= 8; offset++) index.add(offset, 33L * offset);

            assertArrayEquals(indexBytes(0, 0, 4, 132, 8, 264),
                    Files.readAllBytes(temp.root().resolve("segment.index")));
            assertEquals(new IndexEntry(0, 0), index.floor(-1));
            assertEquals(new IndexEntry(0, 0), index.floor(0));
            assertEquals(new IndexEntry(0, 0), index.floor(3));
            assertEquals(new IndexEntry(4, 132), index.floor(4));
            assertEquals(new IndexEntry(4, 132), index.floor(6));
            assertEquals(new IndexEntry(8, 264), index.floor(8));
            assertEquals(new IndexEntry(8, 264), index.floor(Long.MAX_VALUE));
        }
    }

    @Test
    void supportsNonzeroBasesAndReopensPersistedPoints() throws Exception {
        Path indexFile;
        try (TempDirectory temp = new TempDirectory()) {
            indexFile = temp.root().resolve("segment.index");
            try (SparseIndex index = new SparseIndex(indexFile, 8, 2)) {
                assertEquals(new IndexEntry(8, 0), index.floor(7));
                assertEquals(new IndexEntry(8, 0), index.floor(Long.MAX_VALUE));
                index.add(8, 0);
                index.add(9, 33);
                index.add(10, 66);
                index.add(11, 99);
                assertArrayEquals(indexBytes(8, 0, 10, 66), Files.readAllBytes(indexFile));
            }

            try (SparseIndex reopened = new SparseIndex(indexFile, 8, 2)) {
                assertEquals(new IndexEntry(8, 0), reopened.floor(9));
                assertEquals(new IndexEntry(10, 66), reopened.floor(11));
                assertEquals(new IndexEntry(10, 66), reopened.floor(Long.MAX_VALUE));
                reopened.add(11, 99);
                assertArrayEquals(indexBytes(8, 0, 10, 66), Files.readAllBytes(indexFile));
                reopened.add(12, 132);
                byte[] extendedIndex = indexBytes(8, 0, 10, 66, 12, 132);
                assertArrayEquals(extendedIndex, Files.readAllBytes(indexFile));
                assertEquals(new IndexEntry(12, 132), reopened.floor(13));

                assertCode(ErrorCode.INVALID_REQUEST, () -> reopened.add(10, 165));
                assertArrayEquals(extendedIndex, Files.readAllBytes(indexFile));
            }
            try (SparseIndex reopened = new SparseIndex(indexFile, 8, 2)) {
                assertEquals(new IndexEntry(12, 132), reopened.floor(13));
                assertArrayEquals(indexBytes(8, 0, 10, 66, 12, 132), Files.readAllBytes(indexFile));
            }
        }
    }

    @Test
    void rejectedObservationsLeaveTheIndexAndOrderingStateUnchanged() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path indexFile = temp.root().resolve("segment.index");
            try (SparseIndex index = new SparseIndex(indexFile, 4, 2)) {
                index.add(4, 0);
                index.add(5, 33);
                byte[] before = Files.readAllBytes(indexFile);

                assertCode(ErrorCode.INVALID_REQUEST, () -> index.add(5, 66));
                assertArrayEquals(before, Files.readAllBytes(indexFile));
                assertCode(ErrorCode.INVALID_REQUEST, () -> index.add(4, 99));
                assertArrayEquals(before, Files.readAllBytes(indexFile));
                assertCode(ErrorCode.INVALID_REQUEST, () -> index.add(6, -1));
                assertArrayEquals(before, Files.readAllBytes(indexFile));

                index.add(6, 66);
                index.add(7, 99);
                index.add(8, 132);
                assertArrayEquals(indexBytes(4, 0, 6, 66, 8, 132),
                        Files.readAllBytes(indexFile));
            }
        }
    }

    @Test
    void rebuildsMissingTruncatedAndStaleIndexesFromTheCurrentLog() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path logFile = temp.root().resolve("segment.log");
            Path indexFile = temp.root().resolve("segment.index");
            try (SegmentLog log = SegmentLog.create(logFile, 0)) {
                log.append(java.util.stream.IntStream.range(0, 9)
                        .mapToObj(i -> new RecordData(null, new byte[]{(byte) i}, i)).toList());
            }
            List<Long> fullPositions = RecordBytes.positions(logFile);
            byte[] fullIndex = indexBytes(0, fullPositions.get(0),
                    4, fullPositions.get(4), 8, fullPositions.get(8));

            try (SparseIndex missing = new SparseIndex(indexFile, 0, 4)) {
                missing.rebuild(logFile, 0);
                assertArrayEquals(fullIndex, Files.readAllBytes(indexFile));
            }
            Files.delete(indexFile);
            try (SparseIndex deleted = new SparseIndex(indexFile, 0, 4)) {
                deleted.rebuild(logFile, 0);
                assertArrayEquals(fullIndex, Files.readAllBytes(indexFile),
                        "a deleted persisted index must be reconstructed from the complete log");
            }

            Files.write(indexFile, new byte[]{1, 2, 3});
            try (SparseIndex truncated = new SparseIndex(indexFile, 0, 4)) {
                truncated.rebuild(logFile, 0);
                assertArrayEquals(fullIndex, Files.readAllBytes(indexFile));
            }

            byte[] originalLog = Files.readAllBytes(logFile);
            int shorterLength = Math.toIntExact(fullPositions.get(5));
            Files.write(logFile, Arrays.copyOf(originalLog, shorterLength));
            List<Long> shortPositions = RecordBytes.positions(logFile);
            byte[] shortIndex = indexBytes(0, shortPositions.get(0), 4, shortPositions.get(4));

            Files.write(indexFile, fullIndex);
            try (SparseIndex stale = new SparseIndex(indexFile, 0, 4)) {
                stale.rebuild(logFile, 0);
                assertArrayEquals(shortIndex, Files.readAllBytes(indexFile));
                assertEquals(new IndexEntry(4, shortPositions.get(4)), stale.floor(8));
            }

            ByteBuffer malformed = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                    .putLong(-1).putLong(-1);
            Files.write(indexFile, malformed.array());
            try (SparseIndex invalid = new SparseIndex(indexFile, 0, 4)) {
                invalid.rebuild(logFile, 0);
                assertArrayEquals(shortIndex, Files.readAllBytes(indexFile));
                assertEquals(new IndexEntry(4, shortPositions.get(4)), invalid.floor(8));
            }
        }
    }

    @Test
    void rebuildRejectsMalformedLogsWithoutChangingTheirBytes() throws Exception {
        byte[] valid = RecordBytes.record(0, null, new byte[]{7}, 0);
        List<InvalidLog> invalidLogs = List.of(
                new InvalidLog("truncated", Arrays.copyOf(valid, valid.length - 1)),
                new InvalidLog("malformed", RecordBytes.record(0, null, new byte[]{7}, 0, -2, 1)),
                new InvalidLog("bad-crc", RecordBytes.withField(valid, 32, 8, 1, false)),
                new InvalidLog("illegal-length", RecordBytes.withLengthPrefix(valid, -1)),
                new InvalidLog("gapped", RecordBytes.concat(valid,
                        RecordBytes.record(2, null, new byte[]{9}, 2))));

        try (TempDirectory temp = new TempDirectory()) {
            for (InvalidLog invalid : invalidLogs) {
                Path directory = temp.root().resolve(invalid.name());
                Files.createDirectories(directory);
                Path logFile = directory.resolve("segment.log");
                Path indexFile = directory.resolve("segment.index");
                Files.write(logFile, invalid.bytes());
                byte[] logBefore = Files.readAllBytes(logFile);
                try (SparseIndex index = new SparseIndex(indexFile, 0, 1)) {
                    assertCode(ErrorCode.CORRUPT_RECORD, () -> index.rebuild(logFile, 0));
                    assertArrayEquals(logBefore, Files.readAllBytes(logFile), invalid.name());

                    Files.write(logFile, valid);
                    index.rebuild(logFile, 0);
                    assertArrayEquals(indexBytes(0, 0), Files.readAllBytes(indexFile),
                            invalid.name() + " must permit a successful rebuild after the source log is repaired");
                }
            }
        }
    }

    private static byte[] indexBytes(long... offsetPositionPairs) {
        if (offsetPositionPairs.length % 2 != 0) throw new IllegalArgumentException("expected pairs");
        ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES * offsetPositionPairs.length)
                .order(ByteOrder.BIG_ENDIAN);
        for (long value : offsetPositionPairs) bytes.putLong(value);
        return bytes.array();
    }

    private record InvalidLog(String name, byte[] bytes) { }
}
