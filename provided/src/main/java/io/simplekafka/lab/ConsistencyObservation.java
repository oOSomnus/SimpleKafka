package io.simplekafka.lab;

import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;

/** One observed point in a producer/broker/consumer fault experiment. */
public record ConsistencyObservation(
        String phase,
        ErrorCode error,
        ReplicationSnapshot replication,
        Map<Integer, Long> physicalLeo,
        OptionalLong position,
        OptionalLong committedOffset,
        List<LogRecord> visible,
        List<LogRecord> effects,
        Optional<AppendResult> receipt) {
    /** Copies collection structure so later changes cannot rewrite the observation. */
    public ConsistencyObservation {
        Objects.requireNonNull(phase, "phase");
        Objects.requireNonNull(error, "error");
        Objects.requireNonNull(replication, "replication");
        physicalLeo = Map.copyOf(physicalLeo);
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(committedOffset, "committedOffset");
        visible = List.copyOf(visible);
        effects = List.copyOf(effects);
        Objects.requireNonNull(receipt, "receipt");
    }
}
