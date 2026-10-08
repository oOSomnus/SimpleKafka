package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;

import java.nio.ByteBuffer;

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
        throw new ExerciseNotImplementedException(1, "RecordCodec.encode");
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
        throw new ExerciseNotImplementedException(1, "RecordCodec.decode");
    }

    static int encodedSize(RecordData data) {
        if (data == null || data.value() == null || data.timestamp() < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record data is invalid");
        byte[] key = data.key();
        byte[] value = data.value();
        long length = MIN_LENGTH + (key == null ? 0L : key.length) + (long) value.length;
        if (length > MAX_LENGTH)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "encoded record exceeds the 1 MiB limit");
        return Math.toIntExact(length + Integer.BYTES);
    }
}
