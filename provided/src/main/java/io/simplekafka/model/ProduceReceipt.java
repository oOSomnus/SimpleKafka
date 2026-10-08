package io.simplekafka.model;

import java.util.Objects;

public record ProduceReceipt(TopicPartition tp, long firstOffset, long nextOffset) {
    public ProduceReceipt {
        Objects.requireNonNull(tp, "tp");
        if (firstOffset < 0 || nextOffset < firstOffset)
            throw new IllegalArgumentException("invalid receipt range");
    }
}
