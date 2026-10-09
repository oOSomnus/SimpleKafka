package io.simplekafka.model;

import java.util.Arrays;
import java.util.Objects;

/**
 * The key, value, timestamp, and optional producer identity associated with one log record.
 *
 * <p>The caller transfers ownership of key and value arrays at construction and should not modify
 * them afterward. Equality and hashing compare array contents and producer-stamp contents.
 *
 * @param key nullable key bytes; ownership transfers to this value without copying
 * @param value non-null value bytes, which may be empty; ownership transfers without copying
 * @param timestamp nonnegative record timestamp
 * @param producerStamp null for an ordinary record, otherwise its durable idempotent-batch identity
 */
public record RecordData(byte[] key, byte[] value, long timestamp, ProducerStamp producerStamp) {
    /** Creates ordinary, non-idempotent record data without copying its byte arrays. */
    public RecordData(byte[] key, byte[] value, long timestamp) {
        this(key, value, timestamp, null);
    }

    /**
     * Creates record data without copying its byte arrays.
     *
     * @param key nullable key bytes whose ownership transfers to this value
     * @param value non-null value bytes whose ownership transfers to this value
     * @param timestamp record timestamp
     * @param producerStamp null for ordinary records, otherwise durable producer identity
     * @throws NullPointerException if {@code value} is null
     * @throws IllegalArgumentException if {@code timestamp} is negative
     */
    public RecordData {
        Objects.requireNonNull(value, "value");
        if (timestamp < 0) throw new IllegalArgumentException("timestamp must be nonnegative");
    }

    /**
     * Tests equality using the contents of both byte arrays, timestamp, and optional producer
     * stamp.
     *
     * @param other object to compare with this record data
     * @return {@code true} if {@code other} has equal record contents and producer identity
     */
    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof RecordData data
                        && timestamp == data.timestamp
                        && Arrays.equals(key, data.key)
                        && Arrays.equals(value, data.value)
                        && Objects.equals(producerStamp, data.producerStamp);
    }

    /**
     * Returns a hash code derived from array contents, timestamp, and producer stamp.
     *
     * @return the content-based hash code of this record data
     */
    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        result = 31 * result + Long.hashCode(timestamp);
        return 31 * result + Objects.hashCode(producerStamp);
    }
}
