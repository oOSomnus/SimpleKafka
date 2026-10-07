package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Objects;
import java.util.zip.CRC32C;

/** Encoder and decoder for the course's big-endian on-disk record format. */
public final class RecordCodec {
    static final int MIN_LENGTH = 28;
    static final int MAX_LENGTH = 1_048_576;
    static final int FIXED_RECORD_BYTES = 32;

    private RecordCodec() { }

    public static ByteBuffer encode(LogRecord record) {
        if (record == null || record.offset() < 0 || record.data().timestamp() < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record is invalid");
        RecordData data = record.data();
        int totalBytes = encodedSize(data);
        byte[] key = data.key();
        byte[] value = data.value();
        int length = totalBytes - Integer.BYTES;
        ByteBuffer encoded = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(length);
        encoded.putInt(0);
        encoded.putLong(record.offset());
        encoded.putLong(data.timestamp());
        encoded.putInt(key == null ? -1 : key.length);
        encoded.putInt(value.length);
        if (key != null) encoded.put(key);
        encoded.put(value);
        CRC32C crc = new CRC32C();
        ByteBuffer covered = encoded.duplicate();
        covered.position(Integer.BYTES * 2).limit(encoded.position());
        crc.update(covered);
        encoded.putInt(Integer.BYTES, (int) crc.getValue());
        encoded.flip();
        return encoded;
    }

    public static LogRecord decode(ByteBuffer source) {
        Objects.requireNonNull(source, "source");
        ByteBuffer input = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        int start = input.position();
        if (input.remaining() < Integer.BYTES)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "incomplete record length prefix");
        int length = input.getInt();
        if (length < MIN_LENGTH || length > MAX_LENGTH)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "record length is outside the supported range");
        long totalBytesLong = Integer.BYTES + (long) length;
        if (input.remaining() < length)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "incomplete record body");
        int end = Math.toIntExact(start + totalBytesLong);

        ByteBuffer record = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        record.position(start + Integer.BYTES).limit(end);
        ByteBuffer body = record.slice().order(ByteOrder.BIG_ENDIAN);
        int expectedCrc = body.getInt();
        CRC32C crc = new CRC32C();
        ByteBuffer covered = source.duplicate();
        covered.position(start + Integer.BYTES * 2).limit(end);
        crc.update(covered);
        if ((int) crc.getValue() != expectedCrc)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "record CRC32C does not match");

        long offset = body.getLong();
        long timestamp = body.getLong();
        int keyLength = body.getInt();
        int valueLength = body.getInt();
        if (keyLength < -1 || valueLength < 0)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "record contains an invalid byte-array length");
        long expectedPayloadBytes = (keyLength < 0 ? 0L : keyLength) + (long) valueLength;
        if (expectedPayloadBytes != body.remaining())
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "record byte-array lengths do not match its body");
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            body.get(key);
        }
        byte[] value = new byte[valueLength];
        body.get(value);
        try {
            LogRecord decoded = new LogRecord(offset, new RecordData(key, value, timestamp));
            source.position(end);
            return decoded;
        } catch (IllegalArgumentException exception) {
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "record contains an invalid offset or timestamp", exception);
        }
    }

    static int encodedSize(RecordData data) {
        if (data == null || data.value() == null || data.timestamp() < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record data is invalid");
        byte[] key = data.key();
        byte[] value = data.value();
        long length = MIN_LENGTH + (key == null ? 0L : key.length) + (long) value.length;
        if (length > MAX_LENGTH)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "encoded record exceeds the 1 MiB limit");
        return Math.toIntExact(length + Integer.BYTES);
    }
}
