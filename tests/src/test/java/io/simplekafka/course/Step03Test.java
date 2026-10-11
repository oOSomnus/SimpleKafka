package io.simplekafka.course;

import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.value;
import static io.simplekafka.support.TestSupport.values;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;

class Step03Test {
    @Test
    @DisplayName(
            "Reads from trusted record boundaries and validates ranges and whole record budgets")
    void readsFromTrustedHintsAndValidatesRangesAndWholeRecordBudgets() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("0.log");
            List<RecordData> batch =
                    List.of(value("a"), value("b"), value("c"), value("d"), value("e"));
            try (SegmentLog log = SegmentLog.create(file, 0)) {
                assertEquals(new AppendResult(0, 5), log.append(batch));
                assertEquals(
                        List.of(0L, 33L, 66L, 99L, 132L),
                        RecordBytes.positions(file),
                        "five one-byte values must occupy five 33-byte records");
                List<LogRecord> diskRecords = RecordBytes.readRecords(file);
                assertEquals(List.of(0L, 1L, 2L, 3L, 4L), offsets(diskRecords));
                assertEquals(List.of("a", "b", "c", "d", "e"), values(diskRecords));

                assertEquals(List.of(0L), offsets(log.readFrom(0, 0, 5, 33)));
                assertEquals(List.of(1L), offsets(log.readFrom(1, 33, 5, 33)));
                assertEquals(List.of(2L), offsets(log.readFrom(2, 66, 5, 33)));
                assertEquals(
                        List.of(2L, 3L),
                        offsets(log.readFrom(2, 33, 2, 66)),
                        "a hint before the requested offset must not change the target offset");

                for (long invalidHint : new long[] {-1, 166}) {
                    assertCode(
                            ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, invalidHint, 5, 165));
                }
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(2, 99, 5, 165));
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, 165, 5, 165));
                assertEquals(
                        List.of(),
                        log.readFrom(5, 165, 5, 165),
                        "the exact EOF hint is valid for an empty read at LEO");
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.readFrom(-1, 0, 5, 165));
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.readFrom(6, 165, 5, 165));

                assertEquals(List.of(), offsets(log.readFrom(0, 0, 5, 32)));
                assertEquals(List.of(0L), offsets(log.readFrom(0, 0, 5, 33)));
                assertEquals(List.of(0L), offsets(log.readFrom(0, 0, 5, 65)));
                assertEquals(List.of(0L, 1L), offsets(log.readFrom(0, 0, 5, 66)));
                assertEquals(List.of(0L, 1L, 2L, 3L, 4L), offsets(log.readFrom(0, 0, 5, 165)));
                assertEquals(List.of(1L, 2L), offsets(log.readFrom(1, 33, 5, 66)));
                assertEquals(List.of(0L), offsets(log.readFrom(0, 0, 1, 165)));
                assertEquals(List.of(0L, 1L), offsets(log.readFrom(0, 0, 2, 165)));
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, 0, 0, 165));
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, 0, -1, 165));
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, 0, 5, 0));
                assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(0, 0, 5, -1));
            }
        }
    }

    @Test
    @DisplayName("Enforces nonzero base bounds and keeps read paging separate from append position")
    void enforcesNonzeroBaseBoundsAndKeepsReadPagingSeparateFromAppendPosition() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("5.log");
            try (SegmentLog log = SegmentLog.create(file, 5)) {
                log.append(List.of(value("a"), value("b"), value("c"), value("d"), value("e")));
                assertEquals(10, log.logEndOffset());
                assertEquals(List.of(5L, 6L, 7L, 8L, 9L), offsets(log.readFrom(5, 0, 5, 165)));
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.readFrom(4, 0, 5, 165));
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> log.readFrom(11, 165, 5, 165));

                long originalLeo = log.logEndOffset();
                List<Long> pagedOffsets = new ArrayList<>();
                long nextOffset = 5;
                while (nextOffset < originalLeo) {
                    long hint = (nextOffset - 5) * 33;
                    List<LogRecord> page = log.readFrom(nextOffset, hint, 1, 33);
                    assertEquals(
                            1,
                            page.size(),
                            "each per-offset page must return exactly its requested record");
                    pagedOffsets.add(page.get(0).offset());
                    nextOffset = page.get(0).offset() + 1;
                }
                assertEquals(List.of(5L, 6L, 7L, 8L, 9L), pagedOffsets);
                assertEquals(
                        originalLeo,
                        log.logEndOffset(),
                        "reading pages must not advance the append position");

                assertEquals(List.of(9L), offsets(log.readFrom(9, 132, 1, 33)));
                assertEquals(new AppendResult(10, 11), log.append(List.of(value("f"))));
                assertEquals(11, log.logEndOffset());
                assertEquals(
                        List.of(9L, 10L),
                        offsets(log.readFrom(9, 132, 5, 66)),
                        "reading before and after an append must see the contiguous new record");
            }
        }
    }

    @Test
    @DisplayName("Rereads complete records and rejects damage written through another handle")
    void rereadsCompleteRecordsAndRejectsDamageWrittenThroughAnotherHandle() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("0.log");
            try (SegmentLog log = SegmentLog.create(file, 0)) {
                log.append(List.of(value("x")));
                byte[] original = Files.readAllBytes(file);
                byte changedByte = (byte) (original[32] ^ 1);
                try (FileChannel external = FileChannel.open(file, StandardOpenOption.WRITE)) {
                    ByteBuffer changed = ByteBuffer.wrap(new byte[] {changedByte});
                    while (changed.hasRemaining()) external.write(changed, 32);
                }

                assertCode(ErrorCode.CORRUPT_RECORD, () -> log.readFrom(0, 0, 1, 33));
            }
        }
    }

    @Test
    @DisplayName("Starts at a trusted hint and checks only records scanned from that boundary")
    void startsAtTrustedHintWithoutReadingEarlierRecords() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("5.log");
            try (SegmentLog log = SegmentLog.create(file, 5)) {
                log.append(List.of(value("a"), value("a longer second value"), value("tail")));
                List<Long> positions = RecordBytes.positions(file);
                List<LogRecord> records = RecordBytes.readRecords(file);
                byte[] bytes = Files.readAllBytes(file);
                bytes[32] ^= 1;
                Files.write(file, bytes);

                assertEquals(
                        records.subList(2, 3),
                        log.readFrom(7, positions.get(1), 1, 36),
                        "the longer skipped record consumes neither the count nor byte budget");
                assertEquals(records.subList(1, 3), log.readFrom(6, positions.get(1), 2, 4096));
                assertCode(ErrorCode.CORRUPT_RECORD, () -> log.read(5, 3, 4096));

                bytes[Math.toIntExact(positions.get(1)) + 32] ^= 1;
                Files.write(file, bytes);
                assertCode(
                        ErrorCode.CORRUPT_RECORD, () -> log.readFrom(7, positions.get(1), 1, 36));
                assertEquals(records.subList(2, 3), log.readFrom(7, positions.get(2), 1, 36));
                assertEquals(8, log.logEndOffset());
            }
        }
    }

    @Test
    @DisplayName("Rejects offset gaps within the records scanned from a trusted hint")
    void rejectsOffsetGapsWithinScannedRecords() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path file = temp.root().resolve("5.log");
            try (SegmentLog log = SegmentLog.create(file, 5)) {
                log.append(List.of(value("a"), value("b"), value("c"), value("d")));
                byte[] first = RecordBytes.record(5, value("a"));
                byte[] second = RecordBytes.record(6, value("b"));
                byte[] third =
                        RecordBytes.withField(RecordBytes.record(7, value("c")), 8, 8, 8, true);
                byte[] fourth = RecordBytes.record(8, value("d"));
                Files.write(file, RecordBytes.concat(first, second, third, fourth));
                assertCode(ErrorCode.CORRUPT_RECORD, () -> log.readFrom(6, 33, 2, 66));

                first = RecordBytes.withField(first, 8, 6, 8, true);
                Files.write(file, RecordBytes.concat(first, second, third, fourth));
                assertCode(ErrorCode.CORRUPT_RECORD, () -> log.readFrom(5, 0, 1, 33));
            }
        }
    }

    @Test
    @DisplayName("Validates hint ranges and budgets before returning empty reads")
    void validatesHintsAndLimitsForEmptyReads() throws Exception {
        try (TempDirectory temp = new TempDirectory();
                SegmentLog log = SegmentLog.create(temp.root().resolve("5.log"), 5)) {
            assertEquals(List.of(), log.readFrom(5, 0, 1, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(5, -1, 1, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(5, 1, 1, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(5, 0, 0, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(5, 0, 1, 0));
            log.append(List.of(value("a")));
            assertEquals(List.of(), log.readFrom(6, 0, 1, 33));
            assertEquals(List.of(), log.readFrom(6, 33, 1, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(6, 34, 1, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(6, 33, 0, 33));
            assertCode(ErrorCode.INVALID_REQUEST, () -> log.readFrom(6, 33, 1, 0));
        }
    }

    private static List<Long> offsets(List<LogRecord> records) {
        return records.stream().map(LogRecord::offset).toList();
    }
}
