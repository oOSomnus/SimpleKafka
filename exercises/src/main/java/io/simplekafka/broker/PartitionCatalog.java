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
     * Step 10; create fixed partition directories and reopen logs by explicit topic configuration.
     */
    public synchronized void createTopic(String topic, int partitionCount) {
        throw new ExerciseNotImplementedException(10, "PartitionCatalog.createTopic");
    }

    /** Step 10; return this broker's single-node metadata ordered by partition number. */
    public synchronized List<PartitionMetadata> metadata(String topic) {
        throw new ExerciseNotImplementedException(10, "PartitionCatalog.metadata");
    }

    public synchronized PartitionLog partition(TopicPartition tp) {
        requireOpen();
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is required");
        PartitionLog log = partitions.get(tp);
        if (log == null) throw unknownPartition(tp);
        return log;
    }

    public synchronized PartitionBackend backend(TopicPartition tp) {
        requireOpen();
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is required");
        PartitionBackend backend = backends.get(tp);
        if (backend == null) throw unknownPartition(tp);
        return backend;
    }

    public synchronized void installBackend(TopicPartition tp, PartitionBackend backend) {
        requireOpen();
        Objects.requireNonNull(backend, "backend");
        if (tp == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "topic partition is required");
        if (!partitions.containsKey(tp)) throw unknownPartition(tp);
        backends.put(tp, backend);
    }

    public int brokerId() {
        return brokerId;
    }

    public Endpoint endpoint() {
        return endpoint;
    }

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
