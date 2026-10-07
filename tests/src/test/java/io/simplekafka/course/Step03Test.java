package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.RecordCodec;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.support.TempDirectory;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.value;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step03Test {
    @Test
    void readsFromRequestedOffsetAcrossHintsAndHonorsWholeRecordBudgets() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("0.log");
            List<RecordData> batch = List.of(value("zero"), value("one"), value("two"), value("three"), value("four"));
            try (SegmentLog log = SegmentLog.create(file, 0)) {
                log.append(batch);
                int firstBytes = encodedBytes(0, batch.get(0));
                int secondBytes = encodedBytes(1, batch.get(1));
                int thirdBytes = encodedBytes(2, batch.get(2));

                List<LogRecord> hinted = log.readFrom(2, firstBytes, 10, 1_000);
                assertEquals(List.of(2L, 3L, 4L), hinted.stream().map(LogRecord::offset).toList());
                assertEquals(List.of(2L, 3L), log.readFrom(2, 0, 2, 1_000)
                        .stream().map(LogRecord::offset).toList());
                assertTrue(log.readFrom(0, 0, 10, firstBytes - 1).isEmpty(),
                        "a first record that cannot fit must not be split or skipped");
                assertEquals(List.of(0L, 1L), log.readFrom(0, 0, 10, firstBytes + secondBytes)
                        .stream().map(LogRecord::offset).toList());
                assertEquals(List.of(2L), log.readFrom(2, 0, 10, thirdBytes)
                        .stream().map(LogRecord::offset).toList());

                assertTrue(log.readFrom(5, 0, 10, 1_000).isEmpty());
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.readFrom(6, 0, 10, 1_000));
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, 0, 0, 1_000));
                assertCode(ErrorCode.INVALID_REQUEST,
                        () -> log.readFrom(2, firstBytes + secondBytes + thirdBytes, 10, 1_000));

                assertEquals(new AppendResult(5, 6), log.append(List.of(value("after-read"))));
                assertEquals(6, log.logEndOffset(), "reading must not disturb the append position");
            }
        }
    }

    private static int encodedBytes(long offset, RecordData data) {
        return RecordCodec.encode(new LogRecord(offset, data)).remaining();
    }
}
