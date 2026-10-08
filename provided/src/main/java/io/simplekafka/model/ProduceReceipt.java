package io.simplekafka.model;

import java.util.Objects;

/**
 * Receipt for the half-open offset range appended to a topic-partition.
 *
 * @param tp topic-partition receiving the records
 * @param firstOffset inclusive first offset in the receipt range
 * @param nextOffset exclusive next offset after the receipt range
 */
public record ProduceReceipt(TopicPartition tp, long firstOffset, long nextOffset) {
    /**
     * Creates a receipt for a valid half-open offset range.
     *
     * @param tp topic-partition receiving the records
     * @param firstOffset inclusive first offset in the receipt range
     * @param nextOffset exclusive next offset after the receipt range
     * @throws NullPointerException if {@code tp} is null
     * @throws IllegalArgumentException if {@code firstOffset} is negative or
     *     {@code nextOffset} is less than {@code firstOffset}
     */
    public ProduceReceipt {
        Objects.requireNonNull(tp, "tp");
        if (firstOffset < 0 || nextOffset < firstOffset)
            throw new IllegalArgumentException("invalid receipt range");
    }
}
