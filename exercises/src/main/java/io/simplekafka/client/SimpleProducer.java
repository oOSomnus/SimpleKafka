package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.Acks;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.transport.RpcClient;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Synchronous, single-threaded producer with explicit batching; direct {@link RpcClient} is owned,
 * {@link MetadataRouter} is borrowed. Defaults to {@link Acks#LEADER} with a 5,000 ms timeout.
 */
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

    /**
     * Creates a producer that owns the supplied RPC client.
     *
     * @param client RPC client whose ownership transfers to this producer
     * @param partitioner partition-selection strategy
     * @param batchRecords number of records per partition batch before automatic send
     * @throws NullPointerException if {@code client} or {@code partitioner} is null
     * @throws CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST} if {@code
     *     batchRecords} is not positive
     */
    public SimpleProducer(RpcClient client, Partitioner partitioner, int batchRecords) {
        this.client = Objects.requireNonNull(client, "client");
        this.router = null;
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        if (batchRecords <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "batchRecords must be positive");
        this.batchRecords = batchRecords;
    }

    /**
     * Creates a producer that borrows the supplied metadata router.
     *
     * @param router shared router, which remains owned by its caller
     * @param partitioner partition-selection strategy
     * @param batchRecords number of records per partition batch before automatic send
     * @throws NullPointerException if {@code router} or {@code partitioner} is null
     * @throws CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST} if {@code
     *     batchRecords} is not positive
     */
    public SimpleProducer(MetadataRouter router, Partitioner partitioner, int batchRecords) {
        this.client = null;
        this.router = Objects.requireNonNull(router, "router");
        this.partitioner = Objects.requireNonNull(partitioner, "partitioner");
        if (batchRecords <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "batchRecords must be positive");
        this.batchRecords = batchRecords;
    }

    /**
     * Changes the acknowledgement policy and timeout when no batch is pending.
     *
     * @param acks acknowledgement policy for later sends
     * @param timeoutMillis nonnegative acknowledgement timeout in milliseconds
     * @throws IllegalStateException if this producer is closed
     * @throws NullPointerException if {@code acks} is null
     * @throws CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST} if records are
     *     pending or the timeout is negative, or with {@link
     *     io.simplekafka.ErrorCode#REQUEST_TIMEOUT} if a previous produce outcome is unknown
     */
    public void setAcks(Acks acks, long timeoutMillis) {
        ensureOpen();
        ensureKnownOutcome();
        if (!batches.isEmpty())
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "cannot change acks with pending records");
        if (timeoutMillis < 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "timeoutMillis must be nonnegative");
        this.acks = Objects.requireNonNull(acks, "acks");
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Step 13: route and batch one record, sending a partition batch when it reaches the configured
     * threshold. Per-partition FIFO order is preserved; a failed produce is not retried and leaves
     * the outcome unknown. Test: Step13Test. Lesson: docs/book/chapters/04-client-offset.tex, Step
     * 13.
     *
     * @param topic topic to produce to
     * @param key nullable key used for partition routing
     * @param value non-null record value
     * @param timestamp nonnegative record timestamp
     * @throws IllegalStateException if this producer is closed
     * @throws CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for an invalid
     *     topic, null value, negative timestamp, or oversized record; with {@link
     *     io.simplekafka.ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the topic is unknown; or with
     *     {@link io.simplekafka.ErrorCode#REQUEST_TIMEOUT} if a produce outcome is unknown
     * @throws ExerciseNotImplementedException while the Step 13 exercise method is a skeleton
     */
    public void send(String topic, byte[] key, byte[] value, long timestamp) {
        throw new ExerciseNotImplementedException(13, "send");
    }

    /**
     * Step 13: send every nonempty partition batch and return receipts accumulated since the prior
     * flush, then clear that receipt accumulation. Flush is explicit; the default policy is {@link
     * Acks#LEADER}. Test: Step13Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 13.
     *
     * @return an unmodifiable list of receipts produced since the preceding flush
     * @throws IllegalStateException if this producer is closed
     * @throws CourseException with {@link io.simplekafka.ErrorCode#REQUEST_TIMEOUT} if an outcome
     *     is unknown or the acknowledgement wait fails
     * @throws ExerciseNotImplementedException while the Step 13 exercise method is a skeleton
     */
    public List<ProduceReceipt> flush() {
        throw new ExerciseNotImplementedException(13, "flush");
    }

    private void ensureKnownOutcome() {
        if (outcomeUnknown)
            throw new CourseException(
                    ErrorCode.REQUEST_TIMEOUT,
                    "a previous produce outcome is unknown; close this producer");
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("producer is closed");
    }

    /**
     * Closes the producer, clears pending batches, receipts, and cached metadata, and closes only
     * an owned RPC client. A borrowed router remains open; repeated calls have no effect.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        batches.clear();
        receipts.clear();
        directMetadata.clear();
        if (client != null) client.close();
    }
}
