package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
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
    private boolean explicitFlushRequired;
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

    /**
     * Step 13: route and batch one record, sending a partition batch at the configured threshold.
     * Contract: preserve per-partition order and never retry a timed-out batch automatically.
     * Test: Step13Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 13.
     */
    public void send(String topic, byte[] key, byte[] value, long timestamp) {
        throw new ExerciseNotImplementedException(13, "send");
    }

    /**
     * Step 13: send all nonempty batches and return receipts accumulated since the prior flush.
     * Contract: flush is explicit; the producer defaults to LEADER acknowledgments.
     * Test: Step13Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 13.
     */
    public List<ProduceReceipt> flush() {
        throw new ExerciseNotImplementedException(13, "flush");
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
