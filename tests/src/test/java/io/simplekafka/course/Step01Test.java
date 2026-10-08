package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.RecordCodec;
import io.simplekafka.support.RecordBytes;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step01Test {
    private static final HexFormat HEX = HexFormat.of();
    private static final byte[] NULL_KEY_EMPTY_VALUE =
            HEX.parseHex("0000001cdf159f3200000000000000070000000000000017ffffffff00000000");
    private static final byte[] EMPTY_KEY_EMPTY_VALUE =
            HEX.parseHex("0000001cacc2d247000000000000000700000000000000170000000000000000");
    private static final byte[] ONE_BYTE_KEY_AND_VALUE =
            HEX.parseHex("0000001ef5e706790000000000000007000000000000001700000001000000016b76");

    @Test
    @DisplayName("Encode and decode match independent big endian golden records")
    void encodeAndDecodeMatchIndependentBigEndianGoldenRecords() {
        List<LogRecord> records = List.of(
                new LogRecord(7, new RecordData(null, new byte[0], 23)),
                new LogRecord(7, new RecordData(new byte[0], new byte[0], 23)),
                new LogRecord(7, new RecordData(new byte[]{'k'}, new byte[]{'v'}, 23)));
        List<byte[]> goldens = List.of(NULL_KEY_EMPTY_VALUE, EMPTY_KEY_EMPTY_VALUE, ONE_BYTE_KEY_AND_VALUE);
        for (int index = 0; index < records.size(); index++) {
            LogRecord expected = records.get(index);
            byte[] golden = goldens.get(index);
            assertArrayEquals(golden, RecordBytes.record(expected.offset(), expected.data().key(),
                    expected.data().value(), expected.data().timestamp()));
            ByteBuffer encoded = RecordCodec.encode(expected);
            byte[] actual = new byte[encoded.remaining()];
            encoded.get(actual);
            assertArrayEquals(golden, actual, "golden record " + index);
            assertEquals(expected, RecordCodec.decode(ByteBuffer.wrap(golden)), "decoded golden " + index);
        }

        byte[] first = RecordBytes.concat(NULL_KEY_EMPTY_VALUE, EMPTY_KEY_EMPTY_VALUE, ONE_BYTE_KEY_AND_VALUE);
        ByteBuffer input = ByteBuffer.wrap(first);
        assertEquals(records, List.of(RecordCodec.decode(input), RecordCodec.decode(input), RecordCodec.decode(input)));
        assertEquals(0, input.remaining(), "decoding each record must preserve the next record boundary");
    }

    @Test
    @DisplayName("Decodes one record from heap direct read only and slice buffers")
    void decodesOneRecordFromHeapDirectReadOnlyAndSliceBuffers() {
        LogRecord expected = new LogRecord(7, new RecordData(new byte[]{'k'}, new byte[]{'v'}, 23));
        for (ByteBuffer input : bufferViews(ONE_BYTE_KEY_AND_VALUE)) {
            input.order(ByteOrder.LITTLE_ENDIAN);
            int start = input.position();
            int limit = input.limit();
            assertEquals(expected, RecordCodec.decode(input));
            assertEquals(start + ONE_BYTE_KEY_AND_VALUE.length, input.position());
            assertEquals(limit, input.limit());
            assertEquals((byte) 0x7e, input.get(), "trailing sentinel is not consumed");
        }
    }

    @Test
    @DisplayName("Rejects every incomplete prefix and out of range record length")
    void rejectsEveryIncompletePrefixAndOutOfRangeRecordLength() {
        for (int prefixBytes = 0; prefixBytes < ONE_BYTE_KEY_AND_VALUE.length; prefixBytes++) {
            byte[] truncated = java.util.Arrays.copyOf(ONE_BYTE_KEY_AND_VALUE, prefixBytes);
            assertCode(ErrorCode.INVALID_REQUEST, () -> RecordCodec.decode(ByteBuffer.wrap(truncated)));
        }
        for (int length : new int[]{-1, 0, 27, 1_048_577, Integer.MAX_VALUE}) {
            byte[] prefix = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN).putInt(length).array();
            assertCode(ErrorCode.CORRUPT_RECORD, () -> RecordCodec.decode(ByteBuffer.wrap(prefix)));
        }
    }

    @Test
    @DisplayName("Rejects corruption in every stored field")
    void rejectsCorruptionInEveryStoredField() {
        for (int fieldPosition : new int[]{4, 8, 16, 24, 28, 32, 33}) {
            byte[] corrupted = ONE_BYTE_KEY_AND_VALUE.clone();
            corrupted[fieldPosition] ^= 1;
            assertCode(ErrorCode.CORRUPT_RECORD, () -> RecordCodec.decode(ByteBuffer.wrap(corrupted)));
        }
    }

    @Test
    @DisplayName("Rejects structurally invalid fields even when their CRC is valid")
    void rejectsStructurallyInvalidFieldsEvenWhenTheirCrcIsValid() {
        for (byte[] malformed : List.of(
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 8, -1, 8, true),
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 16, -1, 8, true),
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 24, -2, 4, true),
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 28, -1, 4, true),
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 24, 2, 4, true),
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 24, Integer.MAX_VALUE, 4, true),
                RecordBytes.withField(ONE_BYTE_KEY_AND_VALUE, 28, Integer.MAX_VALUE, 4, true))) {
            assertCode(ErrorCode.CORRUPT_RECORD, () -> RecordCodec.decode(ByteBuffer.wrap(malformed)));
        }
    }

    @Test
    @DisplayName("Enforces length limit and supports largest nonnegative logical values")
    void enforcesLengthLimitAndSupportsLargestNonnegativeLogicalValues() {
        byte[] largestValue = new byte[1_048_548];
        RecordData maximumData = new RecordData(null, largestValue, Long.MAX_VALUE);
        LogRecord maximumRecord = new LogRecord(Long.MAX_VALUE, maximumData);
        ByteBuffer encoded = RecordCodec.encode(maximumRecord);
        byte[] independentBytes = RecordBytes.record(Long.MAX_VALUE, null, largestValue, Long.MAX_VALUE);
        byte[] actualBytes = new byte[encoded.remaining()];
        encoded.duplicate().get(actualBytes);
        assertEquals(1_048_580, independentBytes.length);
        assertArrayEquals(independentBytes, actualBytes,
                "the maximum accepted record must match the independent on-disk layout byte for byte");
        assertEquals(1_048_580, encoded.remaining());
        assertEquals(1_048_576, encoded.getInt(encoded.position()));
        assertEquals(maximumRecord, RecordCodec.decode(ByteBuffer.wrap(independentBytes)),
                "the maximum-size independent fixture must decode to the original record");
        assertEquals(maximumRecord, RecordCodec.decode(encoded));

        byte[] tooLargeValue = new byte[largestValue.length + 1];
        assertCode(ErrorCode.INVALID_REQUEST, () ->
                RecordCodec.encode(new LogRecord(0, new RecordData(null, tooLargeValue, 0))));
    }

    private static List<ByteBuffer> bufferViews(byte[] record) {
        ByteBuffer heap = ByteBuffer.allocate(record.length + 5);
        heap.put(new byte[]{1, 2, 3}).put(record).put((byte) 0x7e).flip();
        heap.position(3);
        heap.limit(4 + record.length);

        ByteBuffer direct = ByteBuffer.allocateDirect(record.length + 5);
        direct.put(new byte[]{1, 2, 3}).put(record).put((byte) 0x7e).flip();
        direct.position(3);
        direct.limit(4 + record.length);

        ByteBuffer readOnly = ByteBuffer.wrap(new byte[record.length + 5]);
        readOnly.put(new byte[]{1, 2, 3}).put(record).put((byte) 0x7e).flip();
        readOnly.position(3);
        readOnly.limit(4 + record.length);
        readOnly = readOnly.asReadOnlyBuffer();

        ByteBuffer parent = ByteBuffer.allocate(record.length + 6);
        parent.put(new byte[]{1, 2}).put(record).put((byte) 0x7e).flip();
        parent.position(1);
        parent.limit(record.length + 3);
        ByteBuffer slice = parent.slice();
        slice.position(1);
        slice.limit(record.length + 2);
        return List.of(heap, direct, readOnly, slice);
    }
}
