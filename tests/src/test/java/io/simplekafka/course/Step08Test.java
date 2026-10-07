package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.TempDirectory;
import java.nio.file.Path;
import java.util.Collections;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step08Test {
    @Test
    void truncatesAtRecordBoundariesAndContinuesFromTheTruncatedOffset() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("orders-0");
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                log.append(Collections.nCopies(9, new RecordData(null, new byte[0], 0)));
                log.truncateTo(5);
                assertEquals(5, log.logEndOffset());
                assertEquals(List.of(0L, 1L, 2L, 3L, 4L),
                        log.read(0, 10, 1_024).stream().map(LogRecord::offset).toList());
                assertEquals(List.of(), log.read(5, 1, 128), "reading at LEO returns an empty result");

                assertEquals(new AppendResult(5, 6), log.append(List.of(new RecordData(null, new byte[0], 0))));
                log.truncateTo(log.logEndOffset());
                assertEquals(6, log.logEndOffset(), "truncating to LEO must leave the log unchanged");

                log.truncateTo(0);
                assertEquals(0, log.logStartOffset());
                assertEquals(0, log.logEndOffset());
                assertEquals(new AppendResult(0, 1), log.append(List.of(new RecordData(null, new byte[0], 0))));
            }
        }
    }

    @Test
    void rejectsTruncationOutsideTheRetainedOffsetRange() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             PartitionLog log = new PartitionLog(temp.root().resolve("orders-0"), 96, 1)) {
            log.append(Collections.nCopies(6, new RecordData(null, new byte[0], 0)));
            log.deleteBefore(3);
            assertEquals(3, log.logStartOffset());
            assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.truncateTo(2));
            assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.truncateTo(7));
        }
    }
}
