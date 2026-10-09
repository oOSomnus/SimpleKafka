package io.simplekafka.lab;

import io.simplekafka.model.TopicPartition;

import java.util.List;
import java.util.Objects;

/** Ordered observations collected by one cross-layer fault experiment. */
public record ConsistencyTrace(TopicPartition tp, List<ConsistencyObservation> observations) {
    /** Copies the observation sequence into an immutable list. */
    public ConsistencyTrace {
        Objects.requireNonNull(tp, "tp");
        observations = List.copyOf(observations);
    }
}
