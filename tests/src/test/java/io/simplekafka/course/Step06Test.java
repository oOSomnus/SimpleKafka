package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.TempDirectory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step06Test {
    @Test
    void rollsSegmentsAndReopensWithoutGapsOrDuplicateOffsets() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("orders-0");
            List<RecordData> ten = java.util.Collections.nCopies(10, new RecordData(null, new byte[0], 0));
            try (PartitionLog log = new PartitionLog(directory, 256, 2)) {
                assertEquals(new AppendResult(0, 10), log.append(ten));
                assertEquals(List.of(7L, 8L, 9L),
                        log.read(7, 3, 512).stream().map(LogRecord::offset).toList());
            }

            try (PartitionLog reopened = new PartitionLog(directory, 256, 2)) {
                assertEquals(new AppendResult(10, 11), reopened.append(List.of(new RecordData(null, new byte[0], 0))));
                assertEquals(11, reopened.logEndOffset());
                assertEquals(java.util.stream.LongStream.range(0, 11).boxed().toList(),
                        reopened.read(0, 11, 2_048).stream().map(LogRecord::offset).toList());
            }
        }
    }

    @Test
    void acceptsOneLegalRecordLargerThanTheConfiguredSegment() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             PartitionLog log = new PartitionLog(temp.root().resolve("large-record"), 64, 2)) {
            RecordData record = new RecordData(null, new byte[100], 1);
            assertEquals(new AppendResult(0, 1), log.append(List.of(record)));
            assertEquals(record, log.read(0, 1, 256).getFirst().data());
        }
    }

    @Test
    void invalidBatchDoesNotWriteOrCreateARollOverSegment() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("invalid-batch");
            try (PartitionLog log = new PartitionLog(directory, 64, 1)) {
                long segmentsBefore = segmentCount(directory);
                assertCode(ErrorCode.INVALID_REQUEST,
                        () -> log.append(List.of(new RecordData(null, new byte[0], 0),
                                new RecordData(null, new byte[1_048_577], 0))));
                assertEquals(0, log.logEndOffset());
                assertEquals(segmentsBefore, segmentCount(directory));
            }
        }
    }

    private static long segmentCount(Path directory) throws IOException {
        try (Stream<Path> paths = Files.walk(directory)) {
            return paths.filter(path -> path.getFileName().toString().endsWith(".log")).count();
        }
    }
}
