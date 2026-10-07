package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Single-threaded manual consumer. Direct RpcClient ownership is transferred; a MetadataRouter is borrowed. */
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

    /**
     * Step 14: replace the manual assignment, de-duplicate and sort partitions, and start at zero.
     * Contract: do not consult committed offsets; new positions are zero. Test: Step14Test.
     * Lesson: docs/book/chapters/04-client-offset.tex, Step 14.
     */
    public void assign(Collection<TopicPartition> partitions) {
        throw new ExerciseNotImplementedException(14, "assign");
    }

    /**
     * Step 14: set the local next-fetch position for an assigned partition.
     * Contract: seeking does not commit; seeking an unassigned partition returns NOT_ASSIGNED.
     * Test: Step14Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 14.
     */
    public void seek(TopicPartition tp, long offset) {
        throw new ExerciseNotImplementedException(14, "seek");
    }

    /**
     * Step 14: fetch one bounded round in sorted partition order and advance successful positions.
     * Contract: maxRecords/maxBytes are total budgets; failures do not advance that partition.
     * Test: Step14Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 14.
     */
    public Map<TopicPartition, List<LogRecord>> poll(int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(14, "poll");
    }

    public long position(TopicPartition tp) {
        ensureOpen();
        Long offset = positions.get(Objects.requireNonNull(tp, "tp"));
        if (offset == null) throw new CourseException(ErrorCode.NOT_ASSIGNED, "partition is not assigned: " + tp);
        return offset;
    }

    /**
     * Step 15: assign partitions and initialize positions from committed offsets, defaulting to zero.
     * Contract: this later resume operation is not used by Step 14 assign/poll. Test: Step15Test.
     * Lesson: docs/book/chapters/04-client-offset.tex, Step 15.
     */
    public void resume(Collection<TopicPartition> partitions) {
        throw new ExerciseNotImplementedException(15, "resume");
    }

    /**
     * Step 15: commit each assigned next-fetch position without committing during poll.
     * Contract: commits may move forward or backward and remain group/partition scoped.
     * Test: Step15Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 15.
     */
    public void commitSync() {
        throw new ExerciseNotImplementedException(15, "commitSync");
    }

    void setGroupToken(GroupToken token) { this.token = token; }

    void applyGroupAssignment(Collection<TopicPartition> partitions) {
        resume(partitions);
    }
    Messages.Reply controlCall(Messages.Request request) {
        if (router != null) {
            if (request instanceof Messages.FetchOffsetRequest fetch) router.metadata(fetch.key().tp().topic());
            else if (request instanceof Messages.CommitOffsetRequest commit) router.metadata(commit.key().tp().topic());
            return router.controlCall(request);
        }
        return client.call(request);
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

    @Override public void close() {
        if (closed) return;
        closed = true;
        positions.clear();
        directMetadata.clear();
        if (client != null) client.close();
        token = null;
    }
}
