package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.utf8;
import static io.simplekafka.support.TestSupport.value;
import static io.simplekafka.support.TestSupport.values;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step02Test {
    @Test
    void appendsBatchesFromNonzeroBaseAndPreservesIndependentDiskEncoding() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("5.log");
            RecordData first = new RecordData(utf8("key-a"), utf8("a"), 11);
            RecordData second = new RecordData(utf8("key-b"), utf8("b"), 22);
            RecordData third = new RecordData(utf8("key-c"), utf8("c"), 33);
            try (SegmentLog log = SegmentLog.create(file, 5)) {
                assertEquals(new AppendResult(5, 7), log.append(List.of(first, second)));
                assertEquals(new AppendResult(7, 8), log.append(List.of(third)));
                assertEquals(8, log.logEndOffset());
            }

            byte[] expectedBytes = RecordBytes.concat(
                    RecordBytes.record(5, first.key(), first.value(), first.timestamp()),
                    RecordBytes.record(6, second.key(), second.value(), second.timestamp()),
                    RecordBytes.record(7, third.key(), third.value(), third.timestamp()));
            assertArrayEquals(expectedBytes, Files.readAllBytes(file),
                    "the stored records must match the independent big-endian record fixture");
            List<LogRecord> expectedRecords = List.of(
                    new LogRecord(5, first), new LogRecord(6, second), new LogRecord(7, third));
            List<LogRecord> diskRecords = RecordBytes.readRecords(file);
            assertEquals(expectedRecords, diskRecords, "disk parsing must preserve keys, values, and timestamps");
            assertEquals(List.of(5L, 6L, 7L), diskRecords.stream().map(LogRecord::offset).toList());
            assertEquals(List.of("a", "b", "c"), values(diskRecords));
        }
    }

    @Test
    void rejectsInvalidBatchesAtomicallyWithoutCreatingOffsetGaps() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SegmentLog log = SegmentLog.create(temp.root().resolve("5.log"), 5)) {
            Path file = log.path();
            assertEquals(new AppendResult(5, 8),
                    log.append(List.of(value("a"), value("b"), value("c"))));
            byte[] originalBytes = Files.readAllBytes(file);

            RecordData oversized = new RecordData(null, new byte[1_048_577], 0);
            List<List<RecordData>> invalidBatches = new ArrayList<>();
            invalidBatches.add(List.of());
            invalidBatches.add(null);
            for (int nullPosition = 0; nullPosition < 3; nullPosition++) {
                List<RecordData> withNull = new ArrayList<>(List.of(value("first"), value("second")));
                withNull.add(nullPosition, null);
                invalidBatches.add(withNull);
            }
            invalidBatches.add(List.of(oversized, value("after-first")));
            invalidBatches.add(List.of(value("before-middle"), oversized, value("after-middle")));
            invalidBatches.add(List.of(value("before-last"), oversized));

            for (int index = 0; index < invalidBatches.size(); index++) {
                List<RecordData> invalidBatch = invalidBatches.get(index);
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.append(invalidBatch));
                assertArrayEquals(originalBytes, Files.readAllBytes(file),
                        "rejected batch " + index + " must leave the complete file unchanged");
                assertEquals(8, log.logEndOffset(),
                        "rejected batch " + index + " must not advance the log end offset");
            }

            assertEquals(new AppendResult(8, 9), log.append(List.of(value("d"))),
                    "the next successful append must continue immediately after the original records");
            assertEquals(9, log.logEndOffset());
            byte[] expectedBytes = RecordBytes.concat(
                    RecordBytes.record(5, null, utf8("a"), 0),
                    RecordBytes.record(6, null, utf8("b"), 0),
                    RecordBytes.record(7, null, utf8("c"), 0),
                    RecordBytes.record(8, null, utf8("d"), 0));
            assertArrayEquals(expectedBytes, Files.readAllBytes(file));
            assertEquals(List.of(5L, 6L, 7L, 8L),
                    RecordBytes.readRecords(file).stream().map(LogRecord::offset).toList());
        }
    }

    @Test
    void rejectsAppendAtMaximumOffsetWithoutChangingEmptyFile() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SegmentLog log = SegmentLog.create(temp.root().resolve("max.log"), Long.MAX_VALUE)) {
            Path file = log.path();
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.append(List.of(value("overflow"))));
            assertEquals(Long.MAX_VALUE, log.logEndOffset());
            assertArrayEquals(new byte[0], Files.readAllBytes(file));
        }
    }

    @Test
    void concurrentBatchesReceiveDisjointRangesWithoutInterleaving() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SegmentLog log = SegmentLog.create(temp.root().resolve("0.log"), 0)) {
            ExecutorService workers = Executors.newFixedThreadPool(2);
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            List<RecordData> leftData = List.of(
                    new RecordData(null, utf8("l0"), 101), new RecordData(null, utf8("l1"), 102));
            List<RecordData> rightData = List.of(
                    new RecordData(null, utf8("r0"), 201), new RecordData(null, utf8("r1"), 202));
            try {
                Future<AppendResult> left = workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return log.append(leftData);
                });
                Future<AppendResult> right = workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return log.append(rightData);
                });
                assertTrue(ready.await(2, TimeUnit.SECONDS), "both append workers must reach the start gate");
                start.countDown();

                AppendResult leftResult = left.get(2, TimeUnit.SECONDS);
                AppendResult rightResult = right.get(2, TimeUnit.SECONDS);
                List<AppendResult> ranges = new ArrayList<>(List.of(leftResult, rightResult));
                ranges.sort(Comparator.comparingLong(AppendResult::firstOffset));
                assertEquals(List.of(new AppendResult(0, 2), new AppendResult(2, 4)), ranges,
                        "the returned batch ranges must be disjoint and contiguous");
                assertEquals(4, log.logEndOffset());

                List<LogRecord> expectedDiskRecords = new ArrayList<>();
                for (AppendResult range : ranges) {
                    expectedDiskRecords.addAll(range.equals(leftResult)
                            ? expectedBatchRecords(range, leftData) : expectedBatchRecords(range, rightData));
                }
                List<LogRecord> diskRecords = RecordBytes.readRecords(log.path());
                assertEquals(expectedDiskRecords, diskRecords,
                        "independent disk parsing must preserve each batch's exact values and timestamps");
                assertEquals(expectedBatchRecords(leftResult, leftData), recordsForRange(diskRecords, leftResult));
                assertEquals(expectedBatchRecords(rightResult, rightData), recordsForRange(diskRecords, rightResult));
                assertEquals(List.of(0L, 1L, 2L, 3L),
                        diskRecords.stream().map(LogRecord::offset).toList(),
                        "independent disk parsing must find each offset exactly once");
                assertEquals(List.of(0L, 34L, 68L, 102L), RecordBytes.positions(log.path()));
            } finally {
                start.countDown();
                workers.shutdownNow();
                assertTrue(workers.awaitTermination(2, TimeUnit.SECONDS),
                        "append workers must terminate within the bounded cleanup interval");
            }
        }
    }

    private static List<LogRecord> expectedBatchRecords(AppendResult range, List<RecordData> data) {
        List<LogRecord> records = new ArrayList<>(data.size());
        for (int index = 0; index < data.size(); index++)
            records.add(new LogRecord(range.firstOffset() + index, data.get(index)));
        return List.copyOf(records);
    }

    private static List<LogRecord> recordsForRange(List<LogRecord> records, AppendResult range) {
        int start = Math.toIntExact(range.firstOffset());
        int end = Math.toIntExact(range.nextOffset());
        return List.copyOf(records.subList(start, end));
    }
}
