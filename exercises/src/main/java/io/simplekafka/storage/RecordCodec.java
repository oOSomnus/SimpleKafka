package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;

import java.nio.BufferUnderflowException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.zip.CRC32C;

/** Step 1: encode and decode the fixed on-disk record format. */
public final class RecordCodec {
    static final int MIN_LENGTH = 28;
    static final int MAX_LENGTH = 1_048_576;
    static final int FIXED_RECORD_BYTES = 32;

    private RecordCodec() {}

    /**
     * Step 1: encode one big-endian record with CRC32C; return a flipped buffer containing exactly
     * that record. The CRC covers the timestamp-through-payload fields and excludes the length and
     * CRC fields. See Step01Test and book step 1.
     *
     * @param record record to encode
     * @return a new buffer positioned at zero and limited to the encoded record
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if the record is invalid or
     *     exceeds the 1 MiB record limit
     * @throws ExerciseNotImplementedException while the Step 1 exercise method is a skeleton
     */
    public static ByteBuffer encode(LogRecord record) {
        // validate record
        if (record == null) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record is null");
        }
        if (record.offset() < 0) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "record offset should be greater than 0");
        }
        RecordData data = record.data();
        if (data == null) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record data should not be null");
        }
        if (data.timestamp() < 0) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "timestamp should be greater than 0");
        }
        byte[] key = data.key();
        byte[] value = data.value();
        if (value == null) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "value should not be null");
        }
        int length = 28 + (key == null ? 0 : key.length) + value.length;
        if (length > MAX_LENGTH) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record length exceeds maximum");
        }
        if (length < MIN_LENGTH) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record length exceeds minimum");
        }
        ByteBuffer buffer = ByteBuffer.allocate(length + 4);
        buffer.order(ByteOrder.BIG_ENDIAN);
        buffer.putInt(length);
        buffer.putInt(0); // set crc to 0 first
        buffer.putLong(record.offset());
        buffer.putLong(data.timestamp());
        buffer.putInt(key == null ? -1 : key.length);
        buffer.putInt(value.length);
        if (key != null) buffer.put(key);
        buffer.put(value);
        CRC32C crc32C = new CRC32C();
        ByteBuffer duplicate = buffer.duplicate();
        duplicate.position(8).limit(buffer.position());
        crc32C.update(duplicate);
        buffer.putInt(4, (int) crc32C.getValue());
        buffer.flip();
        return buffer;
    }

    /**
     * Step 1: decode exactly one record, preserve following bytes, and distinguish incomplete input
     * from corrupt data. The source position advances to the record end only after a successful
     * decode, and its byte order is not used. See Step01Test and book step 1.
     *
     * @param source buffer positioned at the record length prefix
     * @return the decoded record
     * @throws NullPointerException if {@code source} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an incomplete prefix or
     *     body, or with {@link ErrorCode#CORRUPT_RECORD} for an invalid length, checksum, field, or
     *     record offset or timestamp
     * @throws ExerciseNotImplementedException while the Step 1 exercise method is a skeleton
     */
    public static LogRecord decode(ByteBuffer source) {
        if (source == null) {
            throw new NullPointerException("source is null");
        }
        ByteBuffer input = source.duplicate().order(ByteOrder.BIG_ENDIAN);
        int startPos = input.position();
        if (input.remaining() < Integer.BYTES) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "incomplete record length prefix");
        }
        int length = input.getInt();
        if (length < MIN_LENGTH || length > MAX_LENGTH) {
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "record length is outside the supported range");
        }
        if (input.remaining() < length) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "incomplete record body");
        }
        int endPos = input.position() + length;
        input.limit(endPos);

        int crc;
        long offset, timestamp;
        byte[] key = null;
        byte[] value;
        try {
            crc = input.getInt();
            offset = input.getLong();
            timestamp = input.getLong();
            int keyLength = input.getInt();
            int valueLength = input.getInt();
            if (offset < 0 || timestamp < 0 || keyLength < -1 || valueLength < 0) {
                throw new CourseException(ErrorCode.CORRUPT_RECORD, "invalid argument");
            }
            long payloadLength = (keyLength < 0 ? 0L : keyLength) + (long) valueLength;
            if (payloadLength != input.remaining()) {
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD,
                        "record byte-array lengths do not match its body");
            }
            if (keyLength != -1) {
                key = new byte[keyLength];
                input.get(key);
            }
            value = new byte[valueLength];
            input.get(value);
        } catch (BufferUnderflowException e) {
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "record structure is incomplete", e);
        }
        ByteBuffer duplicate = input.duplicate();
        duplicate.position(startPos + 8).limit(endPos);
        CRC32C crc32C = new CRC32C();
        crc32C.update(duplicate);
        if (crc != (int) crc32C.getValue()) {
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "crc not match");
        }
        try {
            RecordData recordData = new RecordData(key, value, timestamp);
            LogRecord decoded = new LogRecord(offset, recordData);
            source.position(endPos);
            return decoded;
        } catch (IllegalArgumentException | NullPointerException e) {
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "illegal argument");
        }
    }

    static int encodedSize(RecordData data) {
        if (data == null || data.value() == null || data.timestamp() < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record data is invalid");
        byte[] key = data.key();
        byte[] value = data.value();
        long length =
                MIN_LENGTH
                        + (key == null ? 0L : key.length)
                        + (long) value.length
                        + (data.producerStamp() == null ? 0 : ProducerStamp.ENCODED_OVERHEAD);
        if (length > MAX_LENGTH)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "encoded record exceeds the 1 MiB limit");
        return Math.toIntExact(length + Integer.BYTES);
    }
}
