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

    /**
     * Creates a manual consumer that owns the supplied RPC client. The group identifier must be a
     * valid nonempty course identifier of at most 255 UTF-8 bytes.
     *
     * @param client RPC client whose ownership transfers to this consumer
     * @param group group identifier used for committed offsets
     * @throws NullPointerException if {@code client} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code group} is invalid
     */
    public SimpleConsumer(RpcClient client, String group) {
        this.client = Objects.requireNonNull(client, "client");
        this.router = null;
        this.group = requireGroup(group);
    }

    /**
     * Creates a manual consumer that borrows the supplied metadata router. The group identifier
     * must be a valid nonempty course identifier of at most 255 UTF-8 bytes.
     *
     * @param router shared router, which remains owned by its caller
     * @param group group identifier used for committed offsets
     * @throws NullPointerException if {@code router} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code group} is invalid
     */
    public SimpleConsumer(MetadataRouter router, String group) {
        this.client = null;
        this.router = Objects.requireNonNull(router, "router");
        this.group = requireGroup(group);
    }

    /**
     * Step 14: replace the manual assignment, deduplicate and sort partitions, and initialize only
     * new positions to zero. Retained partitions keep their positions; committed offsets are not
     * consulted, and an invalid element leaves the prior assignment unchanged. Test: Step14Test.
     * Lesson: docs/book/chapters/04-client-offset.tex, Step 14.
     *
     * @param partitions partitions to assign
     * @throws IllegalStateException if this consumer is closed
     * @throws NullPointerException if {@code partitions} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if an element is null
     * @throws ExerciseNotImplementedException while the Step 14 exercise method is a skeleton
     */
    public void assign(Collection<TopicPartition> partitions) {
        throw new ExerciseNotImplementedException(14, "assign");
    }

    /**
     * Step 14: set the local next-fetch position for an assigned partition; seeking does not
     * commit, and a rejected seek leaves all positions unchanged. Test: Step14Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 14.
     *
     * @param tp assigned partition to reposition
     * @param offset nonnegative next-fetch offset
     * @throws IllegalStateException if this consumer is closed
     * @throws NullPointerException if {@code tp} is null
     * @throws CourseException with {@link ErrorCode#NOT_ASSIGNED} if the partition is not assigned,
     *     or with {@link ErrorCode#OFFSET_OUT_OF_RANGE} if {@code offset} is negative
     * @throws ExerciseNotImplementedException while the Step 14 exercise method is a skeleton
     */
    public void seek(TopicPartition tp, long offset) {
        throw new ExerciseNotImplementedException(14, "seek");
    }

    /**
     * Step 14: fetch one bounded round in sorted partition order and advance successful positions.
     * The record and byte limits are total budgets across partitions; a failed partition keeps its
     * prior position even if earlier partitions in the round advanced. Test: Step14Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 14.
     *
     * @param maxRecords positive total record budget for this poll
     * @param maxBytes positive total encoded-byte budget for this poll
     * @return an unmodifiable map from fetched partitions to their records
     * @throws IllegalStateException if this consumer is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for nonpositive limits or an
     *     invalid fetch reply, or with the server's error code if a fetch fails
     * @throws ExerciseNotImplementedException while the Step 14 exercise method is a skeleton
     */
    public Map<TopicPartition, List<LogRecord>> poll(int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(14, "poll");
    }

    /**
     * Returns the current local next-fetch position for an assigned partition.
     *
     * @param tp assigned partition whose position is requested
     * @return the local next-fetch offset
     * @throws IllegalStateException if this consumer is closed
     * @throws NullPointerException if {@code tp} is null
     * @throws CourseException with {@link ErrorCode#NOT_ASSIGNED} if the partition is not assigned
     */
    public long position(TopicPartition tp) {
        ensureOpen();
        Long offset = positions.get(Objects.requireNonNull(tp, "tp"));
        if (offset == null)
            throw new CourseException(ErrorCode.NOT_ASSIGNED, "partition is not assigned: " + tp);
        return offset;
    }

    /**
     * Step 15: replace the deduplicated, sorted assignment and initialize each position from its
     * committed next offset, defaulting to zero. This later resume operation is not used by Step 14
     * assign/poll. Test: Step15Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 15.
     *
     * @param partitions partitions to resume
     * @throws IllegalStateException if this consumer is closed
     * @throws NullPointerException if {@code partitions} is null
     * @throws CourseException if an element is invalid or a committed-offset lookup fails
     * @throws ExerciseNotImplementedException while the Step 15 exercise method is a skeleton
     */
    public void resume(Collection<TopicPartition> partitions) {
        throw new ExerciseNotImplementedException(15, "resume");
    }

    /**
     * Step 15: commit each assigned next-fetch position without committing during poll. Each
     * request carries the current group token (null for a manual consumer); commits may move
     * backward and server rejection propagates unchanged. Test: Step15Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 15.
     *
     * @throws IllegalStateException if this consumer is closed
     * @throws CourseException if the broker rejects a commit or the request fails
     * @throws ExerciseNotImplementedException while the Step 15 exercise method is a skeleton
     */
    public void commitSync() {
        throw new ExerciseNotImplementedException(15, "commitSync");
    }

    void setGroupToken(GroupToken token) {
        this.token = token;
    }

    /**
     * Replaces a group assignment: retained positions survive, revoked partitions are dropped, and
     * only newly assigned partitions resume from their committed next offset or zero.
     */
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

    /**
     * Closes this consumer, clears its local positions and metadata, and closes only an owned RPC
     * client. A borrowed router remains open; repeated calls have no effect.
     */
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
