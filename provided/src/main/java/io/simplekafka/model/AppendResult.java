package io.simplekafka.model;

/**
 * The half-open offset range produced by an append.
 *
 * @param firstOffset inclusive first offset in the appended range
 * @param nextOffset exclusive next offset after the appended range
 */
public record AppendResult(long firstOffset, long nextOffset) {
    /**
     * Creates an append result for a valid half-open offset range.
     *
     * @param firstOffset inclusive first offset in the range
     * @param nextOffset exclusive next offset after the range
     * @throws IllegalArgumentException if {@code firstOffset} is negative or {@code nextOffset} is
     *     less than {@code firstOffset}
     */
    public AppendResult {
        if (firstOffset < 0 || nextOffset < firstOffset)
            throw new IllegalArgumentException("invalid append range");
    }

    /**
     * Returns the number of offsets in this appended range.
     *
     * @return the range length as an {@code int}
     * @throws ArithmeticException if the range length does not fit in an {@code int}
     */
    public int count() {
        return Math.toIntExact(nextOffset - firstOffset);
    }
}
