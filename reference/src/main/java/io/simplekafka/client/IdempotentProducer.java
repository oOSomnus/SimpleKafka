package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Synchronous producer with per-partition sequence state and caller-directed retries. */
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
        ensureOpen();
        long firstSequence = nextSequences.getOrDefault(tp, 0L);
        List<RecordData> frozen =
                validateAndFreeze(tp, records, acks, timeoutMillis, firstSequence);
        if (pending.containsKey(tp))
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST,
                    "partition already has an unresolved producer batch");
        PartitionMetadata route = partitionMetadata(tp);
        Pending batch = new Pending(frozen, acks, timeoutMillis, firstSequence);
        pending.put(tp, batch);
        return deliver(tp, route, batch);
    }

    /** Explicitly refreshes routing and resends the same unresolved batch identity and data. */
    public ProduceReceipt retry(TopicPartition tp) {
        ensureOpen();
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is null");
        Pending batch = pending.get(tp);
        if (batch == null)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "partition has no unresolved producer batch");
        router.refresh(tp.topic());
        PartitionMetadata route = partitionMetadata(tp);
        return deliver(tp, route, batch);
    }

    private List<RecordData> validateAndFreeze(
            TopicPartition tp,
            List<RecordData> records,
            Acks acks,
            long timeoutMillis,
            long firstSequence) {
        if (tp == null
                || records == null
                || records.isEmpty()
                || acks == null
                || timeoutMillis < 0
                || firstSequence < 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid producer batch arguments");
        List<RecordData> frozen;
        try {
            frozen = List.copyOf(records);
        } catch (NullPointerException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "producer batch contains null", exception);
        }
        try {
            Math.addExact(firstSequence, frozen.size());
        } catch (ArithmeticException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "producer sequence range overflows", exception);
        }
        long requestBytes = 46L + tp.topic().getBytes(StandardCharsets.UTF_8).length;
        for (RecordData record : frozen) {
            if (record.producerStamp() != null)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "producer input records must be unstamped");
            long keyBytes = record.key() == null ? 0L : record.key().length;
            long diskBytes = 96L + keyBytes + record.value().length;
            if (diskBytes > 1_048_580L)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "producer record exceeds the 1 MiB limit");
            requestBytes += 16L + keyBytes + record.value().length;
            if (requestBytes > MessageCodec.MAX_FETCH_BYTE_BUDGET + 32L)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "producer request exceeds frame limit");
        }
        return frozen;
    }

    private PartitionMetadata partitionMetadata(TopicPartition tp) {
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is null");
        for (PartitionMetadata metadata : router.metadata(tp.topic()))
            if (metadata.tp().equals(tp)) return metadata;
        throw new CourseException(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic-partition: " + tp);
    }

    private ProduceReceipt deliver(TopicPartition tp, PartitionMetadata route, Pending batch) {
        Messages.IdempotentProduceRequest request =
                new Messages.IdempotentProduceRequest(
                        tp,
                        route.epoch(),
                        batch.acks,
                        batch.timeoutMillis,
                        producerId,
                        producerEpoch,
                        batch.firstSequence,
                        batch.records);
        Messages.ProduceBody body =
                ClientSupport.requireBody(router.call(tp, request), Messages.ProduceBody.class);
        AppendResult result = body.result();
        if (result.nextOffset() - result.firstOffset() != batch.records.size())
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "producer receipt length does not match the batch");
        long nextSequence;
        try {
            nextSequence = Math.addExact(batch.firstSequence, batch.records.size());
        } catch (ArithmeticException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "producer sequence range overflows", exception);
        }
        ProduceReceipt receipt = new ProduceReceipt(tp, result.firstOffset(), result.nextOffset());
        nextSequences.put(tp, nextSequence);
        pending.remove(tp);
        return receipt;
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
