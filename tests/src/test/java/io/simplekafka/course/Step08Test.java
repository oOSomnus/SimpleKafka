package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.Random;
import java.util.TreeMap;
import java.util.stream.LongStream;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step08Test {
    @Test
    void truncatesAtBoundariesWithinSegmentsAtLeoAndAtLogStart() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            assertTruncateScenario(temp.root().resolve("boundary"), 0, 3,
                    List.of(0L, 3L), List.of(96L, 0L), List.of(3, 0), false);
            assertTruncateScenario(temp.root().resolve("within-segment"), 0, 5,
                    List.of(0L, 3L), List.of(96L, 64L), List.of(3, 2), false);
            assertTruncateScenario(temp.root().resolve("leo-no-op"), 0, 9,
                    List.of(0L, 3L, 6L), List.of(96L, 96L, 96L), List.of(3, 3, 3), true);
            assertTruncateScenario(temp.root().resolve("start-zero"), 0, 0,
                    List.of(0L), List.of(0L), List.of(0), false);
            assertTruncateScenario(temp.root().resolve("retained-start"), 3, 3,
                    List.of(3L), List.of(0L), List.of(0), false);
        }
    }

    @Test
    void invalidAndRepeatedTruncationsPreserveTheRetainedFiles() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("invalid");
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                log.append(records(0, 9).stream().map(LogRecord::data).toList());
                assertEquals(1, log.mutationVersion());
                assertEquals(3, log.deleteBefore(3));
                long versionAfterRetention = log.mutationVersion();
                assertEquals(2, versionAfterRetention);
                Path notes = directory.resolve("notes.txt");
                Files.writeString(notes, "keep");
                Map<String, String> before = directorySnapshot(directory);

                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.truncateTo(2));
                assertEquals(before, directorySnapshot(directory));
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.truncateTo(10));
                assertEquals(versionAfterRetention, log.mutationVersion(),
                        "invalid truncations must not invalidate a recovery proof");
                assertEquals(before, directorySnapshot(directory));

                log.truncateTo(5);
                assertEquals(versionAfterRetention + 1, log.mutationVersion(),
                        "a valid truncation bumps the log version once");
                Map<String, String> afterTruncate = directorySnapshot(directory);
                assertEquals(3, log.logStartOffset());
                assertEquals(5, log.logEndOffset());
                assertEquals(records(3, 2), log.read(3, 10, 256));
                log.truncateTo(5);
                assertEquals(versionAfterRetention + 1, log.mutationVersion(),
                        "a repeated no-op truncation does not bump the version");
                assertEquals(afterTruncate, directorySnapshot(directory));
                assertTrue(Files.exists(notes));
                assertEquals(List.of(3L), segmentBases(directory));
                assertEquals(List.of(64L), segmentSizes(directory));
                assertArrayEquals(indexBytes(3, 0, 4, 32), Files.readAllBytes(indexFile(directory, 3)));
            }

            try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
                assertEquals(3, reopened.logStartOffset());
                assertEquals(5, reopened.logEndOffset());
                assertEquals(records(3, 2), reopened.read(3, 10, 256));
                assertTrue(Files.exists(directory.resolve("notes.txt")));
            }
        }
    }

    @Test
    void fixedSeedOperationsMatchAnIndependentSegmentAndOffsetModel() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("model");
            PartitionLog actual = new PartitionLog(directory, LogModel.SEGMENT_BYTES, 1);
            LogModel model = new LogModel();
            Random random = new Random(20_261_007L);
            long timestamp = 0;
            Operation[] operations = Operation.values();
            try {
                for (int iteration = 0; iteration < 150; iteration++) {
                    Operation operation = iteration < operations.length
                            ? operations[iteration] : operations[random.nextInt(operations.length)];
                    switch (operation) {
                        case APPEND -> {
                            int count = 1 + random.nextInt(4);
                            List<RecordData> batch = new ArrayList<>(count);
                            for (int index = 0; index < count; index++)
                                batch.add(new RecordData(null, new byte[0], timestamp++));
                            AppendResult expected = model.append(batch);
                            assertEquals(expected, actual.append(batch), "append operation " + iteration);
                        }
                        case READ -> {
                            long offset = randomOffset(model.start(), model.end(), random);
                            int maxRecords = 1 + random.nextInt(8);
                            int maxBytes = 1 + random.nextInt(200);
                            assertEquals(model.read(offset, maxRecords, maxBytes),
                                    actual.read(offset, maxRecords, maxBytes), "read operation " + iteration);
                        }
                        case DELETE -> {
                            int thresholdRange = Math.toIntExact(model.end() - model.start() + 2);
                            long threshold = model.start() + random.nextInt(thresholdRange);
                            long expectedStart = model.deleteBefore(threshold);
                            assertEquals(expectedStart, actual.deleteBefore(threshold),
                                    "delete operation " + iteration);
                        }
                        case TRUNCATE -> {
                            long target = randomOffset(model.start(), model.end(), random);
                            model.truncateTo(target);
                            actual.truncateTo(target);
                        }
                        case REOPEN -> {
                            actual.close();
                            actual = new PartitionLog(directory, LogModel.SEGMENT_BYTES, 1);
                        }
                    }

                    assertModelMatches(actual, directory, model, "operation " + iteration + " (" + operation + ")");
                    if ((iteration + 1) % 25 == 0) {
                        actual.close();
                        actual = new PartitionLog(directory, LogModel.SEGMENT_BYTES, 1);
                        assertModelMatches(actual, directory, model, "reopen after " + (iteration + 1));
                    }
                }
            } finally {
                actual.close();
            }
        }
    }

    private static void assertTruncateScenario(Path directory, long retainedStart, long target,
                                               List<Long> expectedBases, List<Long> expectedSizes,
                                               List<Integer> expectedCounts, boolean noOp) throws Exception {
        try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
            log.append(records(0, 9).stream().map(LogRecord::data).toList());
            long expectedVersion = log.mutationVersion();
            if (retainedStart != 0) {
                assertEquals(retainedStart, log.deleteBefore(retainedStart));
                expectedVersion++;
            }
            assertEquals(expectedVersion, log.mutationVersion());
            Path notes = directory.resolve("unrelated.txt");
            Files.writeString(notes, "preserve this file");
            Map<String, String> before = directorySnapshot(directory);

            log.truncateTo(target);
            assertEquals(noOp ? expectedVersion : expectedVersion + 1, log.mutationVersion(),
                    "only a valid truncation that changes the log bumps its version");
            if (noOp) assertEquals(before, directorySnapshot(directory));
            assertPartitionState(log, directory, retainedStart, target,
                    expectedBases, expectedSizes, expectedCounts);
            assertTrue(Files.exists(notes));
        }

        List<LogRecord> expectedAfterAppend = new ArrayList<>(records(retainedStart,
                Math.toIntExact(target - retainedStart)));
        RecordData appended = new RecordData(null, new byte[0], target);
        expectedAfterAppend.add(new LogRecord(target, appended));
        try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
            assertEquals(0, reopened.mutationVersion(),
                    "mutation versions are scoped to the currently opened log object");
            assertEquals(new AppendResult(target, target + 1), reopened.append(List.of(appended)));
            assertEquals(1, reopened.mutationVersion());
            assertEquals(retainedStart, reopened.logStartOffset());
            assertEquals(target + 1, reopened.logEndOffset());
            assertEquals(expectedAfterAppend, reopened.read(retainedStart,
                    expectedAfterAppend.size() + 1, (expectedAfterAppend.size() + 1) * 32));
            assertTrue(Files.exists(directory.resolve("unrelated.txt")));
        }
    }

    private static void assertPartitionState(PartitionLog log, Path directory, long start, long end,
                                             List<Long> bases, List<Long> sizes, List<Integer> counts)
            throws IOException {
        assertEquals(start, log.logStartOffset());
        assertEquals(end, log.logEndOffset());
        assertEquals(bases, segmentBases(directory));
        assertEquals(sizes, segmentSizes(directory));
        List<LogRecord> expected = records(start, Math.toIntExact(end - start));
        assertEquals(expected, log.read(start, expected.size() + 1, Math.max(32, (expected.size() + 1) * 32)));
        for (int index = 0; index < bases.size(); index++) {
            long base = bases.get(index);
            int count = counts.get(index);
            assertTrue(Files.exists(logFile(directory, base)));
            assertTrue(Files.exists(indexFile(directory, base)));
            assertArrayEquals(indexBytes(base, count), Files.readAllBytes(indexFile(directory, base)));
            assertEquals(records(base, count), RecordBytes.readRecords(logFile(directory, base)));
        }
        assertEquals(expected, diskRecords(directory));
    }

    private static List<LogRecord> records(long firstOffset, int count) {
        return LongStream.range(firstOffset, firstOffset + count)
                .mapToObj(offset -> new LogRecord(offset, new RecordData(null, new byte[0], offset)))
                .toList();
    }

    private static byte[] indexBytes(long base, int count) {
        ByteBuffer bytes = ByteBuffer.allocate(Long.BYTES * 2 * count).order(ByteOrder.BIG_ENDIAN);
        for (int index = 0; index < count; index++)
            bytes.putLong(base + index).putLong((long) index * 32);
        return bytes.array();
    }

    private static byte[] indexBytes(long offset0, long position0, long offset1, long position1) {
        return ByteBuffer.allocate(Long.BYTES * 4).order(ByteOrder.BIG_ENDIAN)
                .putLong(offset0).putLong(position0).putLong(offset1).putLong(position1).array();
    }

    private static List<Path> segmentFiles(Path directory) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("[0-9]{20}\\.log"))
                    .sorted().toList();
        }
    }

    private static List<Long> segmentBases(Path directory) throws IOException {
        return segmentFiles(directory).stream()
                .map(path -> Long.parseLong(path.getFileName().toString().substring(0, 20)))
                .toList();
    }

    private static List<Long> segmentSizes(Path directory) throws IOException {
        List<Long> sizes = new ArrayList<>();
        for (Path path : segmentFiles(directory)) sizes.add(Files.size(path));
        return List.copyOf(sizes);
    }

    private static List<LogRecord> diskRecords(Path directory) throws IOException {
        List<LogRecord> records = new ArrayList<>();
        for (Path path : segmentFiles(directory)) records.addAll(RecordBytes.readRecords(path));
        return List.copyOf(records);
    }

    private static Map<String, String> directorySnapshot(Path directory) throws IOException {
        Map<String, String> snapshot = new TreeMap<>();
        try (Stream<Path> paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                snapshot.put(path.getFileName().toString(), Files.isDirectory(path) ? "<directory>"
                        : java.util.HexFormat.of().formatHex(Files.readAllBytes(path)));
            }
        }
        return snapshot;
    }

    private static Path logFile(Path directory, long base) {
        return directory.resolve(String.format(java.util.Locale.ROOT, "%020d.log", base));
    }

    private static Path indexFile(Path directory, long base) {
        return directory.resolve(String.format(java.util.Locale.ROOT, "%020d.index", base));
    }

    private static long randomOffset(long start, long end, Random random) {
        return start + random.nextInt(Math.toIntExact(end - start + 1));
    }

    private static void assertModelMatches(PartitionLog actual, Path directory, LogModel model, String message)
            throws IOException {
        assertEquals(model.start(), actual.logStartOffset(), message + " start");
        assertEquals(model.end(), actual.logEndOffset(), message + " end");
        assertEquals(model.allRecords(), actual.read(model.start(),
                Math.toIntExact(model.end() - model.start() + 1),
                Math.max(32, Math.toIntExact((model.end() - model.start() + 1) * 32))),
                message + " retained reads");
        assertEquals(model.allRecords(), diskRecords(directory), message + " independent disk records");
        assertEquals(model.bases(), segmentBases(directory), message + " segment boundaries");
        assertEquals(model.segmentSizes(), segmentSizes(directory), message + " segment byte counts");
        for (ModelSegment segment : model.segments.values()) {
            Path path = logFile(directory, segment.base);
            assertEquals(segment.records, RecordBytes.readRecords(path), message + " segment " + segment.base);
            assertArrayEquals(indexBytes(segment.base, segment.records.size()),
                    Files.readAllBytes(indexFile(directory, segment.base)), message + " index " + segment.base);
        }

        assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> actual.read(model.start() - 1, 1, 32));
        assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> actual.read(model.end() + 1, 1, 32));
        assertCode(ErrorCode.INVALID_REQUEST, () -> actual.read(model.start(), 0, 32));
    }

    private enum Operation { APPEND, READ, DELETE, TRUNCATE, REOPEN }

    private static final class LogModel {
        private static final long SEGMENT_BYTES = 96;
        private static final int RECORD_BYTES = 32;
        private final NavigableMap<Long, ModelSegment> segments = new TreeMap<>();

        private LogModel() {
            segments.put(0L, new ModelSegment(0));
        }

        private AppendResult append(List<RecordData> batch) {
            long firstOffset = end();
            long nextOffset = firstOffset;
            for (RecordData data : batch) {
                ModelSegment active = segments.lastEntry().getValue();
                if (active.bytes > 0 && RECORD_BYTES > SEGMENT_BYTES - active.bytes) {
                    active = new ModelSegment(nextOffset);
                    segments.put(nextOffset, active);
                }
                active.records.add(new LogRecord(nextOffset, data));
                active.bytes += RECORD_BYTES;
                nextOffset++;
            }
            return new AppendResult(firstOffset, nextOffset);
        }

        private long deleteBefore(long threshold) {
            while (segments.size() > 1 && segments.firstEntry().getValue().endOffset() <= threshold)
                segments.pollFirstEntry();
            return start();
        }

        private void truncateTo(long nextOffset) {
            if (nextOffset == end()) return;
            ModelSegment target = segments.floorEntry(nextOffset).getValue();
            target.records.removeIf(record -> record.offset() >= nextOffset);
            target.bytes = (long) target.records.size() * RECORD_BYTES;
            segments.tailMap(target.base, false).clear();
        }

        private List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
            List<LogRecord> result = new ArrayList<>();
            int bytes = 0;
            for (LogRecord record : allRecords()) {
                if (record.offset() < offset) continue;
                if (result.size() == maxRecords || bytes + RECORD_BYTES > maxBytes) break;
                result.add(record);
                bytes += RECORD_BYTES;
            }
            return List.copyOf(result);
        }

        private long start() { return segments.firstKey(); }

        private long end() { return segments.lastEntry().getValue().endOffset(); }

        private List<Long> bases() { return List.copyOf(segments.keySet()); }

        private List<Long> segmentSizes() {
            return segments.values().stream().map(segment -> segment.bytes).toList();
        }

        private List<LogRecord> allRecords() {
            return segments.values().stream().flatMap(segment -> segment.records.stream()).toList();
        }
    }

    private static final class ModelSegment {
        private final long base;
        private final List<LogRecord> records = new ArrayList<>();
        private long bytes;

        private ModelSegment(long base) { this.base = base; }

        private long endOffset() { return base + records.size(); }
    }
}
