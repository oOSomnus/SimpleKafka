package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.storage.RecordCodec;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.support.TempDirectory;
import java.nio.ByteBuffer;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step04Test {
    @Test
    void reopensAtTheLastCompleteRecordAndTruncatesOnlyThePartialTail() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("0.log");
            long completeBytes;
            try (SegmentLog log = SegmentLog.create(file, 0)) {
                log.append(java.util.List.of(value("zero"), value("one")));
                completeBytes = Files.size(file);
            }

            ByteBuffer thirdRecord = RecordCodec.encode(new LogRecord(2, value("partial")));
            byte[] partial = new byte[2];
            thirdRecord.get(partial);
            Files.write(file, partial, StandardOpenOption.APPEND);

            try (SegmentLog recovered = SegmentLog.open(file, 0)) {
                assertEquals(2, recovered.recover());
                assertEquals(completeBytes, Files.size(file));
                assertEquals(new AppendResult(2, 3), recovered.append(java.util.List.of(value("two"))));
                assertEquals(3, recovered.logEndOffset());
            }
        }
    }

    @Test
    void rejectsCompleteCorruptionInvalidLengthAndNoncontiguousOffsets() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path corruptFile = temp.root().resolve("crc.log");
            try (SegmentLog log = SegmentLog.create(corruptFile, 0)) {
                log.append(java.util.List.of(value("complete")));
            }
            byte[] corruptBytes = Files.readAllBytes(corruptFile);
            corruptBytes[corruptBytes.length - 1] ^= 1;
            Files.write(corruptFile, corruptBytes);
            assertCode(ErrorCode.CORRUPT_RECORD, () -> SegmentLog.open(corruptFile, 0));

            Path badLength = temp.root().resolve("length.log");
            Files.write(badLength, ByteBuffer.allocate(Integer.BYTES).putInt(0).array());
            assertCode(ErrorCode.CORRUPT_RECORD, () -> SegmentLog.open(badLength, 0));

            Path gap = temp.root().resolve("gap.log");
            ByteBuffer offsetFive = RecordCodec.encode(new LogRecord(5, value("gap")));
            byte[] gapBytes = new byte[offsetFive.remaining()];
            offsetFive.get(gapBytes);
            Files.write(gap, gapBytes);
            assertCode(ErrorCode.CORRUPT_RECORD, () -> SegmentLog.open(gap, 0));
        }
    }
}
