package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.utf8;
import static io.simplekafka.support.TestSupport.value;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class Step04Test {
    @Test
    void truncatesEveryIncompleteTailPrefixAndKeepsACompleteThirdRecord() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            byte[] first = record(0, "a");
            byte[] second = record(1, "b");
            byte[] third = record(2, "c");
            byte[] completePrefix = RecordBytes.concat(first, second);
            byte[] completeLog = RecordBytes.concat(completePrefix, third);

            for (int prefixLength = 1; prefixLength < third.length; prefixLength++) {
                Path file = temp.root().resolve("partial-" + prefixLength + ".log");
                Files.write(file, completePrefix);
                Files.write(file, Arrays.copyOf(third, prefixLength), StandardOpenOption.APPEND);

                try (SegmentLog recovered = SegmentLog.open(file, 0)) {
                    assertEquals(2, recovered.recover(),
                            "tail prefix length " + prefixLength + " must recover to the last complete offset");
                    assertEquals(2, recovered.logEndOffset());
                    assertEquals(66, Files.size(file),
                            "tail prefix length " + prefixLength + " must be truncated after two records");
                    assertArrayEquals(completePrefix, Files.readAllBytes(file));
                    assertEquals(new AppendResult(2, 3), recovered.append(List.of(value("c"))));
                    assertEquals(3, recovered.logEndOffset());
                }
                assertArrayEquals(completeLog, Files.readAllBytes(file),
                        "recovery followed by append must restore the exact independent three-record bytes");
            }

            Path completeFile = temp.root().resolve("complete-third.log");
            Files.write(completeFile, completeLog);
            try (SegmentLog recovered = SegmentLog.open(completeFile, 0)) {
                assertEquals(3, recovered.recover());
                assertEquals(3, recovered.logEndOffset());
                assertEquals(99, Files.size(completeFile));
                assertArrayEquals(completeLog, Files.readAllBytes(completeFile));
                assertEquals(List.of(0L, 1L, 2L),
                        RecordBytes.readRecords(completeFile).stream().map(LogRecord::offset).toList());
            }
        }
    }

    @Test
    void recoversEmptyAndNonzeroBaseFilesIdempotentlyAndRejectsWrongBase() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            for (long baseOffset : new long[]{0, 5}) {
                Path emptyFile = temp.root().resolve("empty-" + baseOffset + ".log");
                Files.createFile(emptyFile);
                try (SegmentLog empty = SegmentLog.open(emptyFile, baseOffset)) {
                    assertEquals(baseOffset, empty.recover());
                    assertEquals(baseOffset, empty.logEndOffset());
                    assertEquals(0, empty.sizeBytes());
                }
            }

            Path file = temp.root().resolve("base-five.log");
            byte[] first = record(5, "a");
            byte[] second = record(6, "b");
            byte[] third = record(7, "c");
            byte[] completePrefix = RecordBytes.concat(first, second);
            Files.write(file, completePrefix);
            Files.write(file, Arrays.copyOf(third, 17), StandardOpenOption.APPEND);

            try (SegmentLog recovered = SegmentLog.open(file, 5)) {
                assertEquals(7, recovered.recover());
                assertEquals(7, recovered.logEndOffset());
                assertEquals(66, Files.size(file));
                assertArrayEquals(completePrefix, Files.readAllBytes(file));
                assertEquals(7, recovered.recover(), "a repeated recovery must preserve the recovered end offset");
            }
            try (SegmentLog reopened = SegmentLog.open(file, 5)) {
                assertEquals(7, reopened.recover(), "reopening and recovering again must be idempotent");
                assertEquals(new AppendResult(7, 8), reopened.append(List.of(value("c"))));
            }
            byte[] completeLog = RecordBytes.concat(first, second, third);
            assertArrayEquals(completeLog, Files.readAllBytes(file));
            byte[] beforeWrongBase = Files.readAllBytes(file);
            assertCorruptFileUnchanged(file, beforeWrongBase, 6, "opening offset-5 data with base 6");
        }
    }

    @Test
    void rejectsCompleteCorruptionAndOffsetDiscontinuitiesWithoutChangingFileBytes() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            byte[][] valid = {record(0, "a"), record(1, "b"), record(2, "c")};
            for (int index = 0; index < valid.length; index++) {
                byte[] badCrc = valid[index].clone();
                badCrc[badCrc.length - 1] ^= 1;
                assertCorruptFileUnchanged(temp.root().resolve("crc-" + index + ".log"),
                        replaceRecord(valid, index, badCrc), 0, "CRC damage at record " + index);

                byte[] badStructure = RecordBytes.withField(valid[index], 24, -2, 4, true);
                assertCorruptFileUnchanged(temp.root().resolve("structure-" + index + ".log"),
                        replaceRecord(valid, index, badStructure), 0,
                        "CRC-valid invalid key length at record " + index);

                byte[] badLength = RecordBytes.withLengthPrefix(valid[index], 0);
                assertCorruptFileUnchanged(temp.root().resolve("length-" + index + ".log"),
                        replaceRecord(valid, index, badLength), 0, "illegal length at record " + index);
            }

            assertCorruptFileUnchanged(temp.root().resolve("gap-first.log"),
                    records(1, 2, 3), 0, "gapped offset at the first record");
            assertCorruptFileUnchanged(temp.root().resolve("gap-middle.log"),
                    records(0, 2, 3), 0, "gapped offset at the middle record");
            assertCorruptFileUnchanged(temp.root().resolve("gap-last.log"),
                    records(0, 1, 3), 0, "gapped offset at the last record");
            assertCorruptFileUnchanged(temp.root().resolve("duplicate-middle.log"),
                    records(0, 0, 2), 0, "duplicate offset at the middle record");
            assertCorruptFileUnchanged(temp.root().resolve("duplicate-last.log"),
                    records(0, 1, 1), 0, "duplicate offset at the last record");
            assertCorruptFileUnchanged(temp.root().resolve("backward-last.log"),
                    records(0, 1, 0), 0, "backwards offset at the last record");
            assertCorruptFileUnchanged(temp.root().resolve("backward-middle.log"),
                    records(0, 1, 0, 3), 0, "backwards offset at the middle record");
        }
    }

    @Test
    void missingFileOpenReportsStorageErrorWithIoCause() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path missing = temp.root().resolve("missing/segment.log");
            assertFalse(Files.exists(missing), "the fixture path must remain absent before open");
            var exception = assertCode(ErrorCode.STORAGE_ERROR, () -> SegmentLog.open(missing, 0));
            assertInstanceOf(IOException.class, exception.getCause());
        }
    }

    private static byte[] record(long offset, String value) {
        return RecordBytes.record(offset, null, utf8(value), 0);
    }

    private static byte[] records(long... offsets) {
        byte[][] encoded = new byte[offsets.length][];
        for (int index = 0; index < offsets.length; index++) {
            encoded[index] = record(offsets[index], Character.toString((char) ('a' + index)));
        }
        return RecordBytes.concat(encoded);
    }

    private static byte[] replaceRecord(byte[][] records, int index, byte[] replacement) {
        byte[][] changed = records.clone();
        changed[index] = replacement;
        return RecordBytes.concat(changed);
    }

    private static void assertCorruptFileUnchanged(Path file, byte[] bytes, long baseOffset, String description)
            throws Exception {
        byte[] expectedBytes = bytes.clone();
        Files.write(file, expectedBytes);
        assertCode(ErrorCode.CORRUPT_RECORD, () -> {
            try (SegmentLog ignored = SegmentLog.open(file, baseOffset)) {
                throw new AssertionError(description + " unexpectedly opened");
            }
        });
        assertArrayEquals(expectedBytes, Files.readAllBytes(file),
                description + " must leave the complete file unchanged");
    }
}
