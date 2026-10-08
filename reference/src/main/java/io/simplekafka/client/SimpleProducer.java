package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Synchronous, single-threaded producer with explicit batching; direct RpcClient is owned, MetadataRouter borrowed. */
public final class SimpleProducer implements AutoCloseable {
    private static final long DEFAULT_TIMEOUT_MILLIS = 5_000;

    private final RpcClient client;
    private final MetadataRouter router;
    private final Partitioner partitioner;
    private final int batchRecords;
    private final Map<String, List<PartitionMetadata>> directMetadata = new LinkedHashMap<>();
    private final Map<TopicPartition, ArrayList<RecordData>> batches = new LinkedHashMap<>();
    private final ArrayList<ProduceReceipt> receipts = new ArrayList<>();
    private Acks acks = Acks.LEADER;
    private long timeoutMillis = DEFAULT_TIMEOUT_MILLIS;
    private boolean outcomeUnknown;
    private boolean closed;

    public SimpleProducer(RpcClient client, Partitioner partitioner, int batchRecords) {
        this.client = Objects.requireNonNull(client, "client");
        this.router = null;
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        if (batchRecords <= 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "batchRecords must be positive");
        this.batchRecords = batchRecords;
    }

    public SimpleProducer(MetadataRouter router, Partitioner partitioner, int batchRecords) {
        this.client = null;
        this.router = Objects.requireNonNull(router, "router");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        if (batchRecords <= 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "batchRecords must be positive");
        this.batchRecords = batchRecords;
    }

    /** Changes acknowledgments when there is no unsent batch. */
    public void setAcks(Acks acks, long timeoutMillis) {
        ensureOpen();
        ensureKnownOutcome();
        if (!batches.isEmpty()) throw new CourseException(ErrorCode.INVALID_REQUEST, "cannot change acks with pending records");
        if (timeoutMillis < 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "timeoutMillis must be nonnegative");
        this.acks = Objects.requireNonNull(acks, "acks");
        this.timeoutMillis = timeoutMillis;
    }

    public void send(String topic, byte[] key, byte[] value, long timestamp) {
        ensureOpen();
        ensureKnownOutcome();
        if (value == null || timestamp < 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "value and timestamp are invalid");
        int partitions = metadata(topic).size();
        if (partitions <= 0) throw new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "topic has no partitions: " + topic);
        int partition = partitioner.choose(key, partitions);
        TopicPartition tp = new TopicPartition(topic, partition);
        batches.computeIfAbsent(tp, ignored -> new ArrayList<>()).add(new RecordData(key, value, timestamp));
        if (batches.get(tp).size() >= batchRecords) sendBatch(tp);
    }

    public List<ProduceReceipt> flush() {
        ensureOpen();
        ensureKnownOutcome();
        for (TopicPartition tp : new ArrayList<>(batches.keySet())) sendBatch(tp);
        List<ProduceReceipt> completed = List.copyOf(receipts);
        receipts.clear();
        return completed;
    }

    private void sendBatch(TopicPartition tp) {
        ArrayList<RecordData> records = batches.get(tp);
        if (records == null || records.isEmpty()) return;
        PartitionMetadata metadata = partitionMetadata(tp);
        Messages.ProduceRequest request = new Messages.ProduceRequest(tp, metadata.epoch(), acks, timeoutMillis, records);
        try {
            Messages.Reply reply = router == null ? client.call(request) : router.call(tp, request);
            Messages.ProduceBody body = ClientSupport.requireBody(reply, Messages.ProduceBody.class);
            receipts.add(new ProduceReceipt(tp, body.result().firstOffset(), body.result().nextOffset()));
            batches.remove(tp);
        } catch (RuntimeException exception) {
            outcomeUnknown = true;
            throw exception;
        }
    }

    private List<PartitionMetadata> metadata(String topic) {
        try {
            new TopicPartition(topic, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid topic", exception);
        }
        if (router != null) return router.metadata(topic);
        return directMetadata.computeIfAbsent(topic, key ->
                ClientSupport.requireBody(client.call(new Messages.MetadataRequest(key)), Messages.MetadataBody.class).partitions());
    }

    private PartitionMetadata partitionMetadata(TopicPartition tp) {
        for (PartitionMetadata partition : metadata(tp.topic())) if (partition.tp().equals(tp)) return partition;
        throw new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic-partition: " + tp);
    }

    private void ensureKnownOutcome() {
        if (outcomeUnknown) throw new CourseException(ErrorCode.REQUEST_TIMEOUT, "a previous produce outcome is unknown; close this producer");
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("producer is closed");
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        batches.clear();
        receipts.clear();
        directMetadata.clear();
        if (client != null) client.close();
    }
}
