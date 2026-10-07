package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.RecordCodec;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.support.TempDirectory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step02Test {
    @Test
    void appendsNonemptyBatchesAtContiguousOffsetsAndWritesDecodableRecords() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("0.log");
            List<RecordData> firstBatch = List.of(value("a"), value("b"), value("c"));
            try (SegmentLog log = SegmentLog.create(file, 0)) {
                assertEquals(new AppendResult(0, 3), log.append(firstBatch));
                assertEquals(new AppendResult(3, 5), log.append(List.of(value("d"), value("e"))));
                assertEquals(5, log.logEndOffset());
            }

            ByteBuffer disk = ByteBuffer.wrap(Files.readAllBytes(file));
            List<LogRecord> recoveredFromBytes = new ArrayList<>();
            while (disk.hasRemaining()) recoveredFromBytes.add(RecordCodec.decode(disk));
            assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                    recoveredFromBytes.stream().map(LogRecord::offset).toList());
            assertEquals(List.of("a", "b", "c", "d", "e"),
                    recoveredFromBytes.stream().map(record -> new String(record.data().value(), java.nio.charset.StandardCharsets.UTF_8)).toList());
        }
    }

    @Test
    void rejectsEmptyOrOversizedBatchWithoutWritingAnyRecord() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SegmentLog log = SegmentLog.create(temp.root().resolve("0.log"), 0)) {
            Path file = log.path();
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.append(List.of()));
            assertCode(ErrorCode.INVALID_REQUEST,
                    () -> log.append(List.of(value("must-not-be-written"),
                            new RecordData(null, new byte[1_048_577], 0))));
            assertEquals(0, log.logEndOffset());
            assertEquals(0, Files.size(file));
        }
    }

    @Test
    void concurrentBatchesReceiveDisjointContiguousOffsetRanges() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SegmentLog log = SegmentLog.create(temp.root().resolve("0.log"), 0)) {
            ExecutorService workers = Executors.newFixedThreadPool(2);
            CountDownLatch ready = new CountDownLatch(2);
            CountDownLatch start = new CountDownLatch(1);
            try {
                Future<AppendResult> left = workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return log.append(List.of(value("l0"), value("l1")));
                });
                Future<AppendResult> right = workers.submit(() -> {
                    ready.countDown();
                    start.await();
                    return log.append(List.of(value("r0"), value("r1")));
                });
                ready.await();
                start.countDown();
                List<AppendResult> actual = new ArrayList<>(List.of(left.get(), right.get()));
                actual.sort(Comparator.comparingLong(AppendResult::firstOffset));
                assertEquals(List.of(new AppendResult(0, 2), new AppendResult(2, 4)), actual);
                assertEquals(4, log.logEndOffset());
            } finally {
                start.countDown();
                workers.shutdownNow();
            }
        }
    }
}
