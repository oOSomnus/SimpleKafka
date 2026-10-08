package io.simplekafka.model;

/**
 * A sparse-index point mapping a log offset to a byte position.
 *
 * @param offset nonnegative logical record offset
 * @param position nonnegative byte position within the segment
 */
public record IndexEntry(long offset, long position) {
    /**
     * Creates a sparse-index point with nonnegative offset and position.
     *
     * @param offset logical record offset
     * @param position byte position within the segment
     * @throws IllegalArgumentException if either value is negative
     */
    public IndexEntry {
        if (offset < 0 || position < 0)
            throw new IllegalArgumentException("index values must be nonnegative");
    }
}
