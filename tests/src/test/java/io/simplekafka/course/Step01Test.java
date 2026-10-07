package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.RecordCodec;
import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step01Test {
    @Test
    void roundTripsNullKeyEmptyValueAndUtf8Bytes() {
        LogRecord empty = new LogRecord(7, new RecordData(null, new byte[0], 23));
        ByteBuffer encodedEmpty = RecordCodec.encode(empty);
        assertEquals(32, encodedEmpty.remaining());
        assertEquals(empty, RecordCodec.decode(encodedEmpty));
        assertEquals(0, encodedEmpty.remaining());

        RecordData chinese = new RecordData("键".getBytes(StandardCharsets.UTF_8),
                "订单-東京".getBytes(StandardCharsets.UTF_8), 99);
        LogRecord expected = new LogRecord(8, chinese);
        assertEquals(expected, RecordCodec.decode(RecordCodec.encode(expected)));
        assertArrayEquals("键".getBytes(StandardCharsets.UTF_8), expected.data().key());
        assertArrayEquals("订单-東京".getBytes(StandardCharsets.UTF_8), expected.data().value());
    }

    @Test
    void consumesExactlyOneRecordAndLeavesFollowingBytesUntouched() {
        LogRecord expected = new LogRecord(4, new RecordData(new byte[]{1}, new byte[]{2, 3, 4}, 5));
        ByteBuffer encoded = RecordCodec.encode(expected);
        int recordBytes = encoded.remaining();
        ByteBuffer input = ByteBuffer.allocate(recordBytes + 2);
        input.put(encoded).put((byte) 91).put((byte) 92).flip();

        assertEquals(expected, RecordCodec.decode(input));
        assertEquals(recordBytes, input.position());
        assertEquals(91, Byte.toUnsignedInt(input.get()));
        assertEquals(92, Byte.toUnsignedInt(input.get()));
    }

    @Test
    void rejectsCrcCorruptionAndIncompleteRecords() {
        ByteBuffer encoded = RecordCodec.encode(new LogRecord(0,
                new RecordData(null, "payload".getBytes(StandardCharsets.UTF_8), 0)));
        byte[] corrupt = new byte[encoded.remaining()];
        encoded.get(corrupt);
        corrupt[corrupt.length - 1] ^= 1;
        assertCode(ErrorCode.CORRUPT_RECORD, () -> RecordCodec.decode(ByteBuffer.wrap(corrupt)));

        assertCode(ErrorCode.INVALID_REQUEST,
                () -> RecordCodec.decode(ByteBuffer.wrap(new byte[]{0, 0, 0, 28})));
        assertCode(ErrorCode.INVALID_REQUEST,
                () -> RecordCodec.decode(ByteBuffer.wrap(new byte[]{0, 0, 0, 28, 0, 0, 0})));
    }
}
