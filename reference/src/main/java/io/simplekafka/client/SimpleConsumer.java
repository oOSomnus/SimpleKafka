package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;

import java.util.Collection;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Single-threaded manual consumer. Direct RpcClient ownership is transferred; a MetadataRouter is
 * borrowed.
 */
public final class SimpleConsumer implements AutoCloseable {
    private final RpcClient client;
    private final MetadataRouter router;
    private final String group;
    private final Map<TopicPartition, Long> positions = new TreeMap<>();
    private final Map<String, List<PartitionMetadata>> directMetadata = new TreeMap<>();
    private GroupToken token;
    private boolean closed;

    public SimpleConsumer(RpcClient client, String group) {
        this.client = Objects.requireNonNull(client, "client");
        this.router = null;
        this.group = requireGroup(group);
    }

    public SimpleConsumer(MetadataRouter router, String group) {
        this.client = null;
        this.router = Objects.requireNonNull(router, "router");
        this.group = requireGroup(group);
    }

    public void assign(Collection<TopicPartition> partitions) {
        ensureOpen();
        Objects.requireNonNull(partitions, "partitions");
        TreeMap<TopicPartition, Long> replacement = new TreeMap<>();
        try {
            for (TopicPartition tp : partitions) replacement.put(Objects.requireNonNull(tp), 0L);
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid assignment", exception);
        }
        for (TopicPartition tp : replacement.keySet())
            replacement.put(tp, positions.getOrDefault(tp, 0L));
        positions.clear();
        positions.putAll(replacement);
    }

    public void seek(TopicPartition tp, long offset) {
        ensureOpen();
        Objects.requireNonNull(tp, "tp");
        Long current = positions.get(tp);
        if (current == null)
            throw new CourseException(ErrorCode.NOT_ASSIGNED, "partition is not assigned: " + tp);
        if (offset < 0)
            throw new CourseException(ErrorCode.OFFSET_OUT_OF_RANGE, "offset must be nonnegative");
        positions.put(tp, offset);
    }

    /** Performs one bounded fetch round in topic/partition order. */
    public Map<TopicPartition, List<LogRecord>> poll(int maxRecords, int maxBytes) {
        ensureOpen();
        if (maxRecords <= 0 || maxBytes <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "poll limits must be positive");
        Map<TopicPartition, List<LogRecord>> fetched = new LinkedHashMap<>();
        int recordsRemaining = maxRecords;
        int bytesRemaining = maxBytes;
        for (Map.Entry<TopicPartition, Long> assignment : positions.entrySet()) {
            if (recordsRemaining == 0 || bytesRemaining == 0) break;
            TopicPartition tp = assignment.getKey();
            long currentPosition = assignment.getValue();
            PartitionMetadata metadata = partitionMetadata(tp);
            Messages.FetchRequest request =
                    new Messages.FetchRequest(
                            tp,
                            metadata.epoch(),
                            currentPosition,
                            recordsRemaining,
                            bytesRemaining,
                            token);
            Messages.Reply reply = router == null ? client.call(request) : router.call(tp, request);
            Messages.FetchBody body = ClientSupport.requireBody(reply, Messages.FetchBody.class);
            List<LogRecord> records = body.records();
            int usedBytes =
                    validateFetch(records, currentPosition, recordsRemaining, bytesRemaining);
            if (records.isEmpty()) continue;
            positions.put(tp, Math.addExact(records.get(records.size() - 1).offset(), 1));
            fetched.put(tp, records);
            recordsRemaining -= records.size();
            bytesRemaining -= usedBytes;
        }
        return Collections.unmodifiableMap(fetched);
    }

    public long position(TopicPartition tp) {
        ensureOpen();
        Long offset = positions.get(Objects.requireNonNull(tp, "tp"));
        if (offset == null)
            throw new CourseException(ErrorCode.NOT_ASSIGNED, "partition is not assigned: " + tp);
        return offset;
    }

    public void resume(Collection<TopicPartition> partitions) {
        ensureOpen();
        assign(partitions);
        Map<TopicPartition, Long> committed = new TreeMap<>();
        for (TopicPartition tp : positions.keySet()) {
            Messages.OffsetBody body =
                    ClientSupport.requireBody(
                            controlCall(new Messages.FetchOffsetRequest(new OffsetKey(group, tp))),
                            Messages.OffsetBody.class);
            committed.put(tp, body.nextOffset().orElse(0));
        }
        positions.putAll(committed);
    }

    public void commitSync() {
        ensureOpen();
        for (Map.Entry<TopicPartition, Long> position : positions.entrySet()) {
            OffsetKey key = new OffsetKey(group, position.getKey());
            Messages.CommitOffsetRequest request =
                    new Messages.CommitOffsetRequest(key, position.getValue(), token);
            ClientSupport.requireSuccess(controlCall(request));
        }
    }

    void setGroupToken(GroupToken token) {
        this.token = token;
    }

    /** Updates a group assignment, preserving positions for retained partitions. */
    void applyGroupAssignment(Collection<TopicPartition> partitions) {
        ensureOpen();
        Objects.requireNonNull(partitions, "partitions");
        TreeMap<TopicPartition, Long> replacement = new TreeMap<>();
        try {
            for (TopicPartition tp : partitions) {
                Objects.requireNonNull(tp);
                Long existing = positions.get(tp);
                replacement.put(tp, existing);
            }
        } catch (RuntimeException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid group assignment", exception);
        }
        for (Map.Entry<TopicPartition, Long> entry : replacement.entrySet()) {
            if (entry.getValue() != null) continue;
            Messages.OffsetBody body =
                    ClientSupport.requireBody(
                            controlCall(
                                    new Messages.FetchOffsetRequest(
                                            new OffsetKey(group, entry.getKey()))),
                            Messages.OffsetBody.class);
            entry.setValue(body.nextOffset().orElse(0));
        }
        positions.clear();
        positions.putAll(replacement);
    }

    Messages.Reply controlCall(Messages.Request request) {
        if (router != null) {
            if (request instanceof Messages.FetchOffsetRequest fetch)
                router.metadata(fetch.key().tp().topic());
            else if (request instanceof Messages.CommitOffsetRequest commit)
                router.metadata(commit.key().tp().topic());
            return router.controlCall(request);
        }
        return client.call(request);
    }

    private PartitionMetadata partitionMetadata(TopicPartition tp) {
        List<PartitionMetadata> metadata =
                router == null ? directMetadata(tp.topic()) : router.metadata(tp.topic());
        for (PartitionMetadata partition : metadata)
            if (partition.tp().equals(tp)) return partition;
        throw new CourseException(
                ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic-partition: " + tp);
    }

    private List<PartitionMetadata> directMetadata(String topic) {
        return directMetadata.computeIfAbsent(
                topic,
                key ->
                        ClientSupport.requireBody(
                                        client.call(new Messages.MetadataRequest(key)),
                                        Messages.MetadataBody.class)
                                .partitions());
    }

    private static int validateFetch(
            List<LogRecord> records, long position, int maxRecords, int maxBytes) {
        if (records.size() > maxRecords)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "fetch exceeded maxRecords");
        int bytes = 0;
        long expectedOffset = position;
        for (LogRecord record : records) {
            if (record.offset() != expectedOffset)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "fetch returned noncontiguous offsets");
            expectedOffset = Math.addExact(expectedOffset, 1);
            try {
                bytes = Math.addExact(bytes, ClientSupport.recordBytes(record));
            } catch (ArithmeticException exception) {
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "fetch byte count overflow", exception);
            }
            if (bytes > maxBytes)
                throw new CourseException(ErrorCode.INVALID_REQUEST, "fetch exceeded maxBytes");
        }
        return bytes;
    }

    private static String requireGroup(String group) {
        try {
            return new OffsetKey(group, new TopicPartition("validation", 0)).group();
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid group", exception);
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("consumer is closed");
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        positions.clear();
        directMetadata.clear();
        if (client != null) client.close();
        token = null;
    }
}
