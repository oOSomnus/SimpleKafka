package io.simplekafka.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * The key, value, and timestamp associated with one log record.
 *
 * <p>The caller transfers ownership of key and value arrays at construction and should not modify
 * them afterward. Equality and hashing compare array contents.
 *
 * @param key nullable key bytes; ownership transfers to this value without copying
 * @param value non-null value bytes, which may be empty; ownership transfers without copying
 * @param timestamp nonnegative record timestamp
 */
public record RecordData(byte[] key, byte[] value, long timestamp) {
    /**
     * Creates record data without copying its byte arrays.
     *
     * @param key nullable key bytes whose ownership transfers to this value
     * @param value non-null value bytes whose ownership transfers to this value
     * @param timestamp record timestamp
     * @throws NullPointerException if {@code value} is null
     * @throws IllegalArgumentException if {@code timestamp} is negative
     */
    public RecordData {
        Objects.requireNonNull(value, "value");
        if (timestamp < 0) throw new IllegalArgumentException("timestamp must be nonnegative");
    }

    /**
     * Tests equality using the contents of both byte arrays and the timestamp.
     *
     * @param other object to compare with this record data
     * @return {@code true} if {@code other} is record data with equal array contents and timestamp
     */
    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof RecordData data
                        && timestamp == data.timestamp
                        && Arrays.equals(key, data.key)
                        && Arrays.equals(value, data.value);
    }

    /**
     * Returns a hash code derived from the byte-array contents and timestamp.
     *
     * @return the content-based hash code of this record data
     */
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return 31 * result + Long.hashCode(timestamp);
    }
}
