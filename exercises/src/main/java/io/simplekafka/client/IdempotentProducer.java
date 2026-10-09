package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.Acks;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Step 30: synchronous producer with per-partition sequence state and caller-directed retries. */
public final class IdempotentProducer implements AutoCloseable {
    private final MetadataRouter router;
    private final long producerId;
    private final int producerEpoch;
    private final Map<TopicPartition, Long> nextSequences = new HashMap<>();
    private final Map<TopicPartition, Pending> pending = new HashMap<>();
    private boolean closed;

    public IdempotentProducer(MetadataRouter router, long producerId, int producerEpoch) {
        this.router = Objects.requireNonNull(router, "router");
        if (producerId < 0 || producerEpoch < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "producer identity is negative");
        this.producerId = producerId;
        this.producerEpoch = producerEpoch;
    }

    /** Sends one new batch and advances its sequence only after a valid receipt. */
    public ProduceReceipt send(
            TopicPartition tp, List<RecordData> records, Acks acks, long timeoutMillis) {
        throw new ExerciseNotImplementedException(30, "IdempotentProducer.send");
    }

    /** Explicitly refreshes routing and resends the same unresolved batch identity and data. */
    public ProduceReceipt retry(TopicPartition tp) {
        throw new ExerciseNotImplementedException(30, "IdempotentProducer.retry");
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("producer is closed");
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        pending.clear();
        nextSequences.clear();
    }

    private record Pending(
            List<RecordData> records, Acks acks, long timeoutMillis, long firstSequence) {
        private Pending {
            records = List.copyOf(records);
        }
    }
}
