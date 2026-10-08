package io.simplekafka.broker;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.storage.PartitionLog;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Fixed-partition topic metadata and local log ownership for one broker. */
public final class PartitionCatalog implements AutoCloseable {
    private static final long SEGMENT_BYTES = 1L << 20;
    private static final int INDEX_INTERVAL = 16;

    private final Path root;
    private final int brokerId;
    private final Endpoint endpoint;
    private final Map<String, Integer> topics = new TreeMap<>();
    private final Map<TopicPartition, PartitionLog> partitions = new TreeMap<>();
    private final Map<TopicPartition, PartitionBackend> backends = new TreeMap<>();
    private boolean closed;

    /**
     * Creates the catalog root and binds this catalog to one broker endpoint.
     *
     * @param root catalog directory, normalized to an absolute path
     * @param brokerId nonnegative broker identifier
     * @param endpoint endpoint advertised for this broker
     * @throws NullPointerException if {@code root} or {@code endpoint} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code brokerId} is
     *     negative, or with {@link ErrorCode#STORAGE_ERROR} if the root directory cannot be created
     */
    public PartitionCatalog(Path root, int brokerId, Endpoint endpoint) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        if (brokerId < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "brokerId must be nonnegative");
        this.brokerId = brokerId;
        this.endpoint = Objects.requireNonNull(endpoint, "endpoint");
        try {
            Files.createDirectories(this.root);
        } catch (IOException exception) {
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR, "cannot create catalog root", exception);
        }
    }

    /**
     * Step 10: create fixed partition directories and open each partition log with 1 MiB segments
     * and a 16-record index interval. An equal repeat is idempotent and reopens existing data; a
     * different count for an existing topic is rejected. Failure closes newly opened logs without
     * publishing partial catalog state. See Step10Test.
     *
     * @param topic topic name
     * @param partitionCount positive number of partitions
     * @throws IllegalStateException if this catalog is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for invalid topic or count, or
     *     a repeated topic with a different count; with {@link ErrorCode#STORAGE_ERROR} if creating
     *     or opening partition storage fails
     * @throws ExerciseNotImplementedException while the Step 10 exercise method is a skeleton
     */
    public synchronized void createTopic(String topic, int partitionCount) {
        throw new ExerciseNotImplementedException(10, "PartitionCatalog.createTopic");
    }

    /**
     * Step 10: return this broker's single-node metadata ordered by partition number. Each entry
     * advertises this broker as leader, replica, and ISR member at epoch zero, with the configured
     * endpoint. See Step10Test.
     *
     * @param topic topic whose partitions to describe
     * @return the topic's partition metadata in ascending partition order
     * @throws IllegalStateException if this catalog is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an invalid topic, or with
     *     {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the topic is unknown here
     * @throws ExerciseNotImplementedException while the Step 10 exercise method is a skeleton
     */
    public synchronized List<PartitionMetadata> metadata(String topic) {
        throw new ExerciseNotImplementedException(10, "PartitionCatalog.metadata");
    }

    /**
     * Returns the open log for a catalogued partition.
     *
     * @param tp topic-partition key
     * @return the owned partition log
     * @throws IllegalStateException if this catalog is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code tp} is null, or with
     *     {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the partition is not catalogued
     */
    public synchronized PartitionLog partition(TopicPartition tp) {
        requireOpen();
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is required");
        PartitionLog log = partitions.get(tp);
        if (log == null) throw unknownPartition(tp);
        return log;
    }

    /**
     * Returns the current backend for a catalogued partition.
     *
     * @param tp topic-partition key
     * @return the installed partition backend
     * @throws IllegalStateException if this catalog is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code tp} is null, or with
     *     {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the partition is not catalogued
     */
    public synchronized PartitionBackend backend(TopicPartition tp) {
        requireOpen();
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is required");
        PartitionBackend backend = backends.get(tp);
        if (backend == null) throw unknownPartition(tp);
        return backend;
    }

    /**
     * Replaces the backend installed for an existing partition without replacing its log.
     *
     * @param tp topic-partition key
     * @param backend backend to install
     * @throws IllegalStateException if this catalog is closed
     * @throws NullPointerException if {@code backend} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code tp} is null, or with
     *     {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the partition is not catalogued
     */
    public synchronized void installBackend(TopicPartition tp, PartitionBackend backend) {
        requireOpen();
        Objects.requireNonNull(backend, "backend");
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is required");
        if (!partitions.containsKey(tp)) throw unknownPartition(tp);
        backends.put(tp, backend);
    }

    /**
     * Returns this catalog's configured broker identifier.
     *
     * @return the broker identifier
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * Returns the configured endpoint advertised for this broker.
     *
     * @return the broker endpoint
     */
    public Endpoint endpoint() {
        return endpoint;
    }

    /**
     * Returns the normalized absolute catalog root.
     *
     * @return the catalog root path
     */
    public Path root() {
        return root;
    }

    private void requireOpen() {
        if (closed) throw new IllegalStateException("partition catalog is closed");
    }

    private static CourseException unknownPartition(TopicPartition tp) {
        return new CourseException(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic partition " + tp);
    }

    /**
     * Closes all owned partition logs and clears catalog state; repeated calls have no effect.
     * Later operations that require an open catalog throw {@link IllegalStateException}.
     *
     * @throws RuntimeException if an owned log fails to close; later close failures are suppressed
     *     on the first failure
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        RuntimeException failure = null;
        for (PartitionLog log : partitions.values()) {
            try {
                log.close();
            } catch (RuntimeException exception) {
                if (failure == null) failure = exception;
                else failure.addSuppressed(exception);
            }
        }
        partitions.clear();
        backends.clear();
        topics.clear();
        if (failure != null) throw failure;
    }
}
