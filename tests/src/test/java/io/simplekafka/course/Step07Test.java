package io.simplekafka.course;

import io.simplekafka.ErrorCode;
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

class Step07Test {
    @Test
    void deletesOnlyClosedSegmentsAndKeepsTheActiveSegmentAcrossReopen() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("orders-0");
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                log.append(Collections.nCopies(9, new RecordData(null, new byte[0], 0)));
                assertEquals(6, log.deleteBefore(6));
                assertEquals(6, log.logStartOffset());
                assertEquals(9, log.logEndOffset());
                assertEquals(6, log.deleteBefore(9), "the active segment is never deleted");
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.read(5, 1, 256));
            }

            try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
                assertEquals(6, reopened.logStartOffset());
                assertEquals(9, reopened.logEndOffset());
                assertEquals(List.of(6L, 7L, 8L),
                        reopened.read(6, 10, 512).stream().map(LogRecord::offset).toList());
            }
        }
    }
}
