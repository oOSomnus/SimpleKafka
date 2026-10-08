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
     * that record. See Step01Test and book step 1.
     */
    public static ByteBuffer encode(LogRecord record) {
        throw new ExerciseNotImplementedException(1, "RecordCodec.encode");
    }

    /**
     * Step 1: decode exactly one record, preserve following bytes, and distinguish incomplete input
     * from corrupt data. See Step01Test and book step 1.
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
