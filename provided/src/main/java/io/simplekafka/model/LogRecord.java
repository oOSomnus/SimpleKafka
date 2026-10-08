package io.simplekafka.model;

import java.util.Objects;

/**
 * A record stored at a logical offset in a partition log.
 *
 * @param offset nonnegative logical record offset
 * @param data non-null record payload and timestamp
 */
public record LogRecord(long offset, RecordData data) {
    /**
     * Creates a log record with a nonnegative offset and non-null data.
     *
     * @param offset logical record offset
     * @param data record payload and timestamp
     * @throws IllegalArgumentException if {@code offset} is negative
     * @throws NullPointerException if {@code data} is null
     */
    public LogRecord {
        if (offset < 0) throw new IllegalArgumentException("offset must be nonnegative");
        Objects.requireNonNull(data, "data");
    }
}
