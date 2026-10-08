package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Same-topic group consumer with heartbeat fencing; owns a direct RpcClient and borrows a shared
 * MetadataRouter.
 */
public final class GroupConsumer implements AutoCloseable {
    private final MetadataRouter router;
    private final String group;
    private final String member;
    private final SimpleConsumer consumer;
    private String topic;
    private GroupToken token;
    private boolean closed;

    /**
     * Creates a group consumer that owns the supplied RPC client. Group and member identifiers
     * must satisfy the course limits of 1–255 UTF-8 bytes.
     *
     * @param client RPC client whose ownership transfers to this consumer
     * @param group group identifier
     * @param member member identifier
     * @throws NullPointerException if {@code client} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if the group or member
     *     identifier is invalid
     */
    public GroupConsumer(RpcClient client, String group, String member) {
        this.router = null;
        this.group = validateGroup(group, member).group();
        this.member = member;
        this.consumer = new SimpleConsumer(Objects.requireNonNull(client, "client"), group);
    }

    /**
     * Creates a group consumer that borrows the supplied metadata router. Group and member
     * identifiers must satisfy the course limits of 1–255 UTF-8 bytes.
     *
     * @param router shared router, which remains owned by its caller
     * @param group group identifier
     * @param member member identifier
     * @throws NullPointerException if {@code router} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if the group or member
     *     identifier is invalid
     */
    public GroupConsumer(MetadataRouter router, String group, String member) {
        this.router = Objects.requireNonNull(router, "router");
        this.group = validateGroup(group, member).group();
        this.member = member;
        this.consumer = new SimpleConsumer(router, group);
    }

    /**
     * Step 20: join this same-topic group and establish the member's assignment and token.
     * {@code subscribe} is the explicit way to rejoin after {@link ErrorCode#UNKNOWN_MEMBER};
     * router-backed consumers refresh topic metadata before joining. Retained positions survive,
     * while newly assigned partitions resume from committed offsets or zero. Test: Step20Test.
     * Lesson: docs/book/chapters/05-consumer-groups.tex, Step 20.
     *
     * @param topic topic to join
     * @throws IllegalStateException if this consumer is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an invalid topic or with
     *     {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the topic is unknown, or with
     *     {@link ErrorCode#UNKNOWN_MEMBER} if this member is absent from the returned assignment
     * @throws ExerciseNotImplementedException while the Step 20 exercise method is a skeleton
     */
    public void subscribe(String topic) {
        throw new ExerciseNotImplementedException(20, "subscribe");
    }

    /**
     * Step 20: heartbeat, refresh a stale generation's assignment, then fetch with the current
     * token. Do not auto-rejoin after {@link ErrorCode#UNKNOWN_MEMBER}; revoked positions are
     * discarded and new positions resume from committed offsets or zero. Test: Step20Test. Lesson:
     * docs/book/chapters/05-consumer-groups.tex, Step 20.
     *
     * @param maxRecords positive total record budget
     * @param maxBytes positive total encoded-byte budget
     * @return an unmodifiable map of fetched records keyed by assigned partition
     * @throws IllegalStateException if this consumer is closed
     * @throws CourseException if called before subscribing, a heartbeat or fetch is rejected, or
     *     the limits are invalid
     * @throws ExerciseNotImplementedException while the Step 20 exercise method is a skeleton
     */
    public Map<TopicPartition, List<LogRecord>> poll(int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(20, "poll");
    }

    /**
     * Step 20: commit positions with the current group token and server-side ownership fence. A
     * stale generation or non-owner commit leaves stored offsets unchanged. Test: Step20Test.
     * Lesson: docs/book/chapters/05-consumer-groups.tex, Step 20.
     *
     * @throws IllegalStateException if this consumer is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if called before subscribing,
     *     with {@link ErrorCode#ILLEGAL_GENERATION} for a stale token, or with
     *     {@link ErrorCode#NOT_ASSIGNED} if the member does not own a committed partition
     * @throws ExerciseNotImplementedException while the Step 20 exercise method is a skeleton
     */
    public void commitSync() {
        throw new ExerciseNotImplementedException(20, "commitSync");
    }

    private Messages.Reply controlCall(Messages.Request request) {
        return consumer.controlCall(request);
    }

    private static GroupToken validateGroup(String group, String member) {
        try {
            return new GroupToken(group, member, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid group/member identifier", exception);
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("group consumer is closed");
    }

    /**
     * Closes the inner consumer, clears the group token and topic, and closes only an owned RPC
     * client. A borrowed router remains open; repeated calls have no effect.
     */
    @Override
    public void close() {
        if (closed) return;
        closed = true;
        consumer.close();
        token = null;
        topic = null;
    }
}
