package io.simplekafka.course;

import static io.simplekafka.support.TestSupport.assertCode;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class Step06Test {
    @Test
    @DisplayName("Rolls at exact byte boundaries and writes independent records")
    void rollsAtExactByteBoundariesAndWritesIndependentRecords() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path exactDirectory = temp.root().resolve("exact-rollover");
            List<LogRecord> three = expectedRecords(0, 3);
            try (PartitionLog log = new PartitionLog(exactDirectory, 64, 1)) {
                assertEquals(0, log.mutationVersion());
                assertEquals(
                        new AppendResult(0, 3),
                        log.append(three.stream().map(LogRecord::data).toList()));
                assertEquals(
                        1,
                        log.mutationVersion(),
                        "one valid append batch bumps the in-memory version once");
                assertEquals(List.of(0L, 2L), segmentBases(exactDirectory));
                assertEquals(List.of(64L, 32L), segmentSizes(exactDirectory));
                assertDiskRecords(exactDirectory, three);
            }

            Path batchDirectory = temp.root().resolve("batch-rollover");
            List<LogRecord> eight = expectedRecords(0, 8);
            try (PartitionLog log = new PartitionLog(batchDirectory, 96, 1)) {
                assertEquals(
                        new AppendResult(0, 8),
                        log.append(eight.stream().map(LogRecord::data).toList()));
                assertEquals(
                        1,
                        log.mutationVersion(),
                        "a batch spanning segments bumps the version only once");
                assertEquals(List.of(0L, 3L, 6L), segmentBases(batchDirectory));
                assertEquals(List.of(96L, 96L, 64L), segmentSizes(batchDirectory));
                assertDiskRecords(batchDirectory, eight);
            }
        }
    }

    @Test
    @DisplayName("Applies read budgets across segments and validates bounds and limits")
    void appliesReadBudgetsAcrossSegmentsAndValidatesBoundsAndLimits() throws Exception {
        try (TempDirectory temp = new TempDirectory();
                PartitionLog log = new PartitionLog(temp.root().resolve("orders-0"), 256, 2)) {
            List<LogRecord> expected = expectedRecords(0, 10);
            log.append(expected.stream().map(LogRecord::data).toList());

            assertEquals(expected.subList(0, 8), log.read(0, 20, 256));
            assertEquals(expected.subList(0, 8), log.read(0, 20, 257));
            assertEquals(expected.subList(0, 9), log.read(0, 20, 288));
            assertEquals(expected.subList(1, 9), log.read(1, 20, 280));
            assertEquals(
                    expected.subList(2, 9),
                    log.read(2, 7, 512),
                    "the records budget must be cumulative across segment boundaries");
            assertEquals(expected.subList(7, 10), log.read(7, 3, 512));
            assertEquals(List.of(), log.read(10, 10, 128));

            assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.read(-1, 1, 32));
            assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.read(11, 1, 32));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.read(0, 0, 32));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.read(0, -1, 32));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.read(0, 1, 0));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.read(0, 1, -1));
        }
    }

    @Test
    @DisplayName("Uses the sparse index boundary without rereading the earlier segment prefix")
    void startsReadsAtTheSparseIndexBoundary() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("indexed-read");
            try (PartitionLog log = new PartitionLog(directory, 4096, 2)) {
                List<LogRecord> expected = expectedRecords(0, 6);
                log.append(expected.stream().map(LogRecord::data).toList());
                Path file = directory.resolve(segmentName(0));
                byte[] bytes = Files.readAllBytes(file);
                bytes[16] ^= 1;
                Files.write(file, bytes);

                assertEquals(expected.subList(5, 6), log.read(5, 1, 32));
                assertCode(ErrorCode.CORRUPT_RECORD, () -> log.read(0, 1, 32));

                long indexedPosition = RecordBytes.positions(file).get(4);
                bytes[Math.toIntExact(indexedPosition) + 16] ^= 1;
                Files.write(file, bytes);
                assertCode(ErrorCode.CORRUPT_RECORD, () -> log.read(5, 1, 32));
            }
        }
    }

    @Test
    @DisplayName("Invalid batch does not mutate an existing multi segment log")
    void invalidBatchDoesNotMutateAnExistingMultiSegmentLog() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("invalid-batch");
            try (PartitionLog log = new PartitionLog(directory, 64, 1)) {
                log.append(expectedRecords(0, 5).stream().map(LogRecord::data).toList());
                assertEquals(List.of(0L, 2L, 4L), segmentBases(directory));
                Map<String, String> before = directorySnapshot(directory);
                long versionBefore = log.mutationVersion();

                RecordData oversized = new RecordData(null, new byte[1_048_577], 11);
                List<List<RecordData>> invalidBatches = new ArrayList<>();
                invalidBatches.add(List.of());
                invalidBatches.add(null);
                for (int nullPosition = 0; nullPosition < 3; nullPosition++) {
                    List<RecordData> withNull =
                            new ArrayList<>(
                                    List.of(
                                            new RecordData(null, new byte[0], 10),
                                            new RecordData(null, new byte[0], 12)));
                    withNull.add(nullPosition, null);
                    invalidBatches.add(withNull);
                }
                invalidBatches.add(List.of(oversized, new RecordData(null, new byte[0], 12)));
                invalidBatches.add(
                        List.of(
                                new RecordData(null, new byte[0], 10),
                                oversized,
                                new RecordData(null, new byte[0], 12)));
                invalidBatches.add(List.of(new RecordData(null, new byte[0], 10), oversized));

                for (int index = 0; index < invalidBatches.size(); index++) {
                    List<RecordData> invalid = invalidBatches.get(index);
                    assertCode(ErrorCode.INVALID_REQUEST, () -> log.append(invalid));
                    assertEquals(
                            5,
                            log.logEndOffset(),
                            "rejected batch " + index + " must not advance the log end offset");
                    assertEquals(
                            before,
                            directorySnapshot(directory),
                            "rejected batch "
                                    + index
                                    + " must preserve all segment and index bytes");
                    assertEquals(
                            versionBefore,
                            log.mutationVersion(),
                            "rejected batch " + index + " must not invalidate a recovery proof");
                }

                RecordData appended = new RecordData(null, new byte[0], 12);
                assertEquals(new AppendResult(5, 6), log.append(List.of(appended)));
                assertEquals(
                        versionBefore + 1,
                        log.mutationVersion(),
                        "one valid append after rejected batches increments the version exactly once");
                List<LogRecord> expected = new ArrayList<>(expectedRecords(0, 5));
                expected.add(new LogRecord(5, appended));
                assertDiskRecords(directory, expected);
                assertEquals(6, log.logEndOffset());
            }
        }
    }

    @Test
    @DisplayName(
            "Accepts an oversized record between records in one batch and reopens every segment")
    void acceptsAnOversizedRecordBetweenRecordsInOneBatchAndReopensEverySegment() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("large-record-batch");
            List<RecordData> batch =
                    List.of(
                            new RecordData(null, new byte[0], 0),
                            new RecordData(null, new byte[100], 1),
                            new RecordData(null, new byte[0], 2));
            List<LogRecord> expected =
                    List.of(
                            new LogRecord(0, batch.get(0)),
                            new LogRecord(1, batch.get(1)),
                            new LogRecord(2, batch.get(2)));

            try (PartitionLog log = new PartitionLog(directory, 64, 2)) {
                assertEquals(new AppendResult(0, 3), log.append(batch));
                assertEquals(List.of(0L, 1L, 2L), segmentBases(directory));
                assertEquals(List.of(32L, 132L, 32L), segmentSizes(directory));
                assertMixedSizeReadBudgets(log, expected);
            }

            try (PartitionLog reopened = new PartitionLog(directory, 64, 2)) {
                assertEquals(3, reopened.logEndOffset());
                assertMixedSizeReadBudgets(reopened, expected);
                assertEquals(List.of(0L, 1L, 2L), segmentBases(directory));
                assertEquals(List.of(32L, 132L, 32L), segmentSizes(directory));
            }
        }
    }

    @Test
    @DisplayName("Indexes and reads mixed size records inside one segment")
    void indexesAndReadsMixedSizeRecordsInsideOneSegment() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("mixed-size-single-segment");
            List<RecordData> batch =
                    List.of(
                            new RecordData(null, new byte[0], 0),
                            new RecordData(null, new byte[100], 1),
                            new RecordData(null, new byte[0], 2));
            List<LogRecord> expected =
                    List.of(
                            new LogRecord(0, batch.get(0)),
                            new LogRecord(1, batch.get(1)),
                            new LogRecord(2, batch.get(2)));
            byte[] expectedLog =
                    RecordBytes.concat(
                            RecordBytes.record(0, null, new byte[0], 0),
                            RecordBytes.record(1, null, new byte[100], 1),
                            RecordBytes.record(2, null, new byte[0], 2));
            byte[] expectedIndex = indexBytes(0, 0, 1, 32, 2, 164);
            Path logFile = directory.resolve(segmentName(0));
            Path indexFile = directory.resolve(segmentName(0).replace(".log", ".index"));

            try (PartitionLog log = new PartitionLog(directory, 300, 1)) {
                assertEquals(new AppendResult(0, 3), log.append(batch));
                assertEquals(List.of(0L), segmentBases(directory));
                assertEquals(List.of(196L), segmentSizes(directory));
                assertArrayEquals(expectedLog, Files.readAllBytes(logFile));
                assertArrayEquals(expectedIndex, Files.readAllBytes(indexFile));
                assertEquals(List.of(expected.get(1)), log.read(1, 1, 132));
                assertEquals(List.of(expected.get(2)), log.read(2, 1, 32));
                assertEquals(expected, log.read(0, 3, 196));
                assertDiskRecords(directory, expected);
            }

            Files.delete(indexFile);
            try (PartitionLog reopened = new PartitionLog(directory, 300, 1)) {
                assertEquals(3, reopened.logEndOffset());
                assertEquals(List.of(0L), segmentBases(directory));
                assertEquals(List.of(196L), segmentSizes(directory));
                assertArrayEquals(expectedLog, Files.readAllBytes(logFile));
                assertArrayEquals(expectedIndex, Files.readAllBytes(indexFile));
                assertEquals(List.of(expected.get(1)), reopened.read(1, 1, 132));
                assertEquals(List.of(expected.get(2)), reopened.read(2, 1, 32));
                assertEquals(expected, reopened.read(0, 3, 196));
                assertDiskRecords(directory, expected);
            }
        }
    }

    @Test
    @DisplayName("Recovers only an incomplete active tail and rebuilds indexes on reopen")
    void recoversOnlyAnIncompleteActiveTailAndRebuildsIndexesOnReopen() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("recover");
            List<LogRecord> expected = expectedRecords(0, 5);
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                log.append(expected.stream().map(LogRecord::data).toList());
            }

            Path activeLog = segmentFiles(directory).getLast();
            Files.write(activeLog, new byte[] {0, 0, 0}, StandardOpenOption.APPEND);
            try (PartitionLog recovered = new PartitionLog(directory, 96, 1)) {
                assertEquals(5, recovered.logEndOffset());
                assertEquals(64, Files.size(activeLog));
                assertEquals(expected, recovered.read(0, 5, 160));
            }

            Path baseZeroIndex = directory.resolve(segmentName(0).replace(".log", ".index"));
            Path baseThreeIndex = directory.resolve(segmentName(3).replace(".log", ".index"));
            Files.delete(baseZeroIndex);
            Files.write(baseThreeIndex, new byte[] {1, 2, 3});
            Path notes = directory.resolve("notes.txt");
            Path backup = directory.resolve("00000000000000000007.log.bak");
            Path tempIndex = directory.resolve("index.tmp");
            Files.writeString(notes, "keep me");
            Files.write(backup, new byte[] {4, 5});
            Files.write(tempIndex, new byte[] {6});
            Path numericLogDirectory = Files.createDirectory(directory.resolve(segmentName(99)));
            Path marker = numericLogDirectory.resolve("not-a-segment");
            Files.writeString(marker, "keep directory");

            try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
                assertEquals(5, reopened.logEndOffset());
                assertEquals(expected, reopened.read(0, 5, 160));
                assertEquals(List.of(0L, 3L), segmentBases(directory));
                assertTrue(Files.exists(notes));
                assertTrue(Files.exists(backup));
                assertTrue(Files.exists(tempIndex));
                assertTrue(Files.exists(marker));
                assertDiskRecords(directory, expected);
            }
        }
    }

    @Test
    @DisplayName("Refuses complete CRC corruption and segment base gaps without changing logs")
    void refusesCompleteCrcCorruptionAndSegmentBaseGapsWithoutChangingLogs() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path corruptDirectory = temp.root().resolve("crc-corrupt");
            try (PartitionLog log = new PartitionLog(corruptDirectory, 96, 1)) {
                log.append(expectedRecords(0, 6).stream().map(LogRecord::data).toList());
            }
            List<Path> logFiles = segmentFiles(corruptDirectory);
            assertEquals(List.of(0L, 3L), segmentBases(corruptDirectory));
            byte[] badCrc = Files.readAllBytes(logFiles.getFirst());
            badCrc[badCrc.length - 1] ^= 1;
            Files.write(logFiles.getFirst(), badCrc);
            Map<String, String> corruptBefore = directorySnapshot(corruptDirectory);
            assertCode(ErrorCode.CORRUPT_RECORD, () -> new PartitionLog(corruptDirectory, 96, 1));
            assertEquals(corruptBefore, directorySnapshot(corruptDirectory));
            Path gapDirectory = temp.root().resolve("base-gap");
            Files.createDirectories(gapDirectory);
            Path first = gapDirectory.resolve(segmentName(0));
            Path gap = gapDirectory.resolve(segmentName(2));
            Files.write(first, RecordBytes.record(0, null, new byte[0], 0));
            Files.write(gap, RecordBytes.record(2, null, new byte[0], 2));
            Map<String, String> logFilesBefore = logFileSnapshot(gapDirectory);
            assertCode(ErrorCode.CORRUPT_RECORD, () -> new PartitionLog(gapDirectory, 96, 1));
            assertEquals(logFilesBefore, logFileSnapshot(gapDirectory));
        }
    }

    @Test
    @DisplayName("Concurrent batches stay contiguous and survive reopen")
    void concurrentBatchesStayContiguousAndSurviveReopen() throws Exception {
        int threadCount = 4;
        int batchesPerThread = 8;
        int recordsPerBatch = 5;
        List<LogRecord> expectedRecords = new ArrayList<>();
        Path directory;
        List<Thread> workers = new ArrayList<>();
        CountDownLatch ready = new CountDownLatch(threadCount);
        CountDownLatch start = new CountDownLatch(1);
        AtomicReference<Throwable> workerFailure = new AtomicReference<>();
        List<BatchAppend> results = java.util.Collections.synchronizedList(new ArrayList<>());

        try (TempDirectory temp = new TempDirectory()) {
            directory = temp.root().resolve("concurrent");
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                for (int threadId = 0; threadId < threadCount; threadId++) {
                    int id = threadId;
                    Thread worker =
                            new Thread(
                                    () -> {
                                        ready.countDown();
                                        try {
                                            if (!start.await(2, TimeUnit.SECONDS))
                                                throw new AssertionError(
                                                        "concurrent append start latch timed out");
                                            for (int batch = 0; batch < batchesPerThread; batch++) {
                                                long marker = id * 10_000L + batch * 10L;
                                                List<RecordData> records =
                                                        LongStream.range(0, recordsPerBatch)
                                                                .mapToObj(
                                                                        index ->
                                                                                new RecordData(
                                                                                        null,
                                                                                        new byte[0],
                                                                                        marker
                                                                                                + index))
                                                                .toList();
                                                AppendResult range = log.append(records);
                                                results.add(
                                                        new BatchAppend(id, batch, marker, range));
                                            }
                                        } catch (Throwable failure) {
                                            workerFailure.compareAndSet(null, failure);
                                        }
                                    },
                                    "partition-append-" + threadId);
                    worker.setDaemon(true);
                    workers.add(worker);
                    worker.start();
                }
                try {
                    assertTrue(
                            ready.await(2, TimeUnit.SECONDS),
                            "append workers did not reach the start latch");
                } finally {
                    start.countDown();
                    joinWorkers(workers);
                }

                for (Thread worker : workers)
                    assertFalse(worker.isAlive(), "append worker did not terminate");
                if (workerFailure.get() != null)
                    throw new AssertionError(
                            "concurrent append worker failed", workerFailure.get());
                assertEquals(threadCount * batchesPerThread, results.size());

                List<BatchAppend> sortedResults = new ArrayList<>(results);
                sortedResults.sort(
                        java.util.Comparator.comparingLong(result -> result.range().firstOffset()));
                List<AppendResult> expectedRanges =
                        LongStream.range(0, (long) threadCount * batchesPerThread)
                                .mapToObj(
                                        batch ->
                                                new AppendResult(
                                                        batch * recordsPerBatch,
                                                        (batch + 1) * recordsPerBatch))
                                .toList();
                assertEquals(
                        expectedRanges,
                        sortedResults.stream().map(BatchAppend::range).toList(),
                        "sorted returned ranges must exactly tile offsets [0,160) by complete batches");

                List<Long> expectedMarkers = new ArrayList<>();
                for (int threadId = 0; threadId < threadCount; threadId++) {
                    for (int batch = 0; batch < batchesPerThread; batch++)
                        expectedMarkers.add(threadId * 10_000L + batch * 10L);
                }
                expectedMarkers.sort(Long::compareTo);
                assertEquals(
                        expectedMarkers,
                        sortedResults.stream().map(BatchAppend::marker).sorted().toList(),
                        "each worker batch marker must appear exactly once");

                for (BatchAppend result : sortedResults) {
                    assertEquals(
                            result.threadId() * 10_000L + result.batchIndex() * 10L,
                            result.marker(),
                            "the returned range must remain associated with its worker batch marker");
                    for (int index = 0; index < recordsPerBatch; index++) {
                        expectedRecords.add(
                                new LogRecord(
                                        result.range().firstOffset() + index,
                                        new RecordData(
                                                null, new byte[0], result.marker() + index)));
                    }
                }
                assertEquals(
                        LongStream.range(0, 160).boxed().toList(),
                        expectedRecords.stream().map(LogRecord::offset).toList());
                List<Long> expectedSegmentBases =
                        LongStream.range(0, 54).map(base -> base * 3).boxed().toList();
                List<Long> expectedSegmentSizes =
                        LongStream.range(0, 54).map(index -> index < 53 ? 96 : 32).boxed().toList();
                assertEquals(expectedSegmentBases, segmentBases(directory));
                assertEquals(expectedSegmentSizes, segmentSizes(directory));

                List<LogRecord> disk = diskRecords(directory);
                assertEquals(
                        expectedRecords,
                        disk,
                        "independent disk parsing must preserve the exact data assigned to every sorted range");
                assertEquals(expectedRecords, log.read(0, 160, 5_120));
            }

            try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
                assertEquals(expectedRecords, reopened.read(0, 160, 5_120));
                assertDiskRecords(directory, expectedRecords);
            }
        }
    }

    private static void assertMixedSizeReadBudgets(PartitionLog log, List<LogRecord> expected) {
        assertEquals(List.of(), log.read(0, 3, 31));
        assertEquals(List.of(expected.get(0)), log.read(0, 3, 100));
        assertEquals(List.of(), log.read(1, 3, 131));
        assertEquals(List.of(expected.get(1)), log.read(1, 3, 132));
        assertEquals(expected.subList(1, 3), log.read(1, 3, 164));
        assertEquals(expected.subList(0, 2), log.read(0, 3, 164));
        assertEquals(expected, log.read(0, 3, 196));
    }

    private static List<LogRecord> expectedRecords(long firstOffset, int count) {
        return LongStream.range(firstOffset, firstOffset + count)
                .mapToObj(
                        offset -> new LogRecord(offset, new RecordData(null, new byte[0], offset)))
                .toList();
    }

    private static byte[] indexBytes(long... offsetPositionPairs) {
        ByteBuffer bytes =
                ByteBuffer.allocate(Long.BYTES * offsetPositionPairs.length)
                        .order(ByteOrder.BIG_ENDIAN);
        for (long value : offsetPositionPairs) bytes.putLong(value);
        return bytes.array();
    }

    private static List<Path> segmentFiles(Path directory) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("[0-9]{20}\\.log"))
                    .sorted()
                    .toList();
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

    private static void assertDiskRecords(Path directory, List<LogRecord> expected)
            throws IOException {
        assertEquals(expected, diskRecords(directory));
    }

    private static Map<String, String> directorySnapshot(Path directory) throws IOException {
        Map<String, String> snapshot = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path path : paths.filter(candidate -> !candidate.equals(directory)).toList()) {
                String name = directory.relativize(path).toString();
                snapshot.put(
                        name,
                        Files.isDirectory(path)
                                ? "<directory>"
                                : HexFormat.of().formatHex(Files.readAllBytes(path)));
            }
        }
        return snapshot;
    }

    private static Map<String, String> logFileSnapshot(Path directory) throws IOException {
        Map<String, String> snapshot = new TreeMap<>();
        for (Path path : segmentFiles(directory))
            snapshot.put(
                    path.getFileName().toString(),
                    HexFormat.of().formatHex(Files.readAllBytes(path)));
        return snapshot;
    }

    private static String segmentName(long base) {
        return String.format(java.util.Locale.ROOT, "%020d.log", base);
    }

    private static void joinWorkers(List<Thread> workers) throws InterruptedException {
        for (Thread worker : workers) worker.join(2_000);
        for (Thread worker : workers) if (worker.isAlive()) worker.interrupt();
        for (Thread worker : workers) if (worker.isAlive()) worker.join(2_000);
    }

    private record BatchAppend(int threadId, int batchIndex, long marker, AppendResult range) {}
}
