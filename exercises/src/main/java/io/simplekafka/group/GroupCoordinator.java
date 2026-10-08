package io.simplekafka.group;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.TimeSource;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;

/** Single-process, same-topic group membership, assignment, heartbeat, and generation fence. */
public final class GroupCoordinator implements AutoCloseable {
    private final PartitionCatalog catalog;
    private final RoundRobinAssignor assignor;
    private final TimeSource time;
    private final long sessionTimeoutMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, GroupState> groups = new TreeMap<>();
    private boolean closed;

    /**
     * Creates a coordinator using the supplied catalog, assignment policy, and clock.
     *
     * @param catalog catalog used to resolve the group's topic partitions
     * @param assignor policy used to distribute partitions among members
     * @param time clock used to measure heartbeats and session expiry
     * @param sessionTimeoutMillis positive member session timeout in milliseconds
     * @throws NullPointerException if {@code catalog}, {@code assignor}, or {@code time} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if the timeout is nonpositive
     */
    public GroupCoordinator(
            PartitionCatalog catalog,
            RoundRobinAssignor assignor,
            TimeSource time,
            long sessionTimeoutMillis) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.assignor = Objects.requireNonNull(assignor, "assignor");
        this.time = Objects.requireNonNull(time, "time");
        if (sessionTimeoutMillis <= 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "sessionTimeoutMillis must be positive");
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }

    /**
     * Step 18: add or refresh a same-topic member and immediately rebalance the group. Contract: a
     * new member increments generation; an idempotent join only refreshes its heartbeat, and a
     * rejected join does not consume a generation. Test: Step18Test. Lesson:
     * docs/book/chapters/05-consumer-groups.tex, Step 18.
     *
     * @param group consumer group identifier
     * @param member member identifier
     * @param topic the group's single topic
     * @return the assignment for the current group generation
     * @throws IllegalStateException if this coordinator is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for invalid identifiers or a
     *     group attempting to join a second topic, or with {@link
     *     ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the topic is unknown
     * @throws ExerciseNotImplementedException while the Step 18 exercise method is a skeleton
     */
    public GroupAssignment join(String group, String member, String topic) {
        throw new ExerciseNotImplementedException(18, "join");
    }

    /**
     * Step 18: remove a known member and rebalance every remaining member. Unknown members fail;
     * each leave increments the generation, while an empty group retains its generation counter and
     * committed offsets. Test: Step18Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step
     * 18.
     *
     * @param group consumer group identifier
     * @param member member identifier to remove
     * @return the assignment after the member leaves
     * @throws IllegalStateException if this coordinator is closed
     * @throws CourseException with {@link ErrorCode#UNKNOWN_MEMBER} if the group or member is
     *     unknown
     * @throws ExerciseNotImplementedException while the Step 18 exercise method is a skeleton
     */
    public GroupAssignment leave(String group, String member) {
        throw new ExerciseNotImplementedException(18, "leave");
    }

    /**
     * Step 18: return the current full-group assignment for a known member. Unknown groups or
     * members return {@link ErrorCode#UNKNOWN_MEMBER}. Test: Step18Test. Lesson:
     * docs/book/chapters/05-consumer-groups.tex, Step 18.
     *
     * @param group consumer group identifier
     * @param member member identifier
     * @return the current assignment for the group
     * @throws IllegalStateException if this coordinator is closed
     * @throws CourseException with {@link ErrorCode#UNKNOWN_MEMBER} if the group or member is
     *     unknown
     * @throws ExerciseNotImplementedException while the Step 18 exercise method is a skeleton
     */
    public GroupAssignment assignment(String group, String member) {
        throw new ExerciseNotImplementedException(18, "assignment");
    }

    /**
     * Step 19: validate membership and generation before recording a heartbeat at the injected
     * time. Stale or unknown tokens do not refresh a member's deadline. Test: Step19Test. Lesson:
     * docs/book/chapters/05-consumer-groups.tex, Step 19.
     *
     * @param token member token whose heartbeat is being recorded
     * @throws IllegalStateException if this coordinator is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for a null token, with {@link
     *     ErrorCode#UNKNOWN_MEMBER} for an unknown member, or with {@link
     *     ErrorCode#ILLEGAL_GENERATION} for a stale generation
     * @throws ExerciseNotImplementedException while the Step 19 exercise method is a skeleton
     */
    public void heartbeat(GroupToken token) {
        throw new ExerciseNotImplementedException(19, "heartbeat");
    }

    /**
     * Step 19: expire all due members and return sorted group/member identifiers. Expiry occurs
     * when elapsed time is at least {@code sessionTimeoutMillis}; each expired member advances the
     * generation, and an empty group retains its generation counter and committed offsets. Test:
     * Step19Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 19.
     *
     * @return the expired identifiers in ascending group-then-member order, formatted as {@code
     *     group/member}
     * @throws IllegalStateException if this coordinator is closed
     * @throws ExerciseNotImplementedException while the Step 19 exercise method is a skeleton
     */
    public Set<String> expire() {
        throw new ExerciseNotImplementedException(19, "expire");
    }

    /**
     * Step 20: fence a token by current generation and verify that its member owns the partition. A
     * stale token returns {@link ErrorCode#ILLEGAL_GENERATION}; a current non-owner returns {@link
     * ErrorCode#NOT_ASSIGNED}. Test: Step20Test. Lesson: docs/book/chapters/05-consumer-groups.tex,
     * Step 20.
     *
     * @param token group member token to validate
     * @param tp partition being accessed
     * @throws IllegalStateException if this coordinator is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code tp} is null, with
     *     {@link ErrorCode#UNKNOWN_MEMBER} for an unknown member, with {@link
     *     ErrorCode#ILLEGAL_GENERATION} for a stale token, or with {@link ErrorCode#NOT_ASSIGNED}
     *     if the member does not own {@code tp}
     * @throws ExerciseNotImplementedException while the Step 20 exercise method is a skeleton
     */
    public void validate(GroupToken token, TopicPartition tp) {
        throw new ExerciseNotImplementedException(20, "validate");
    }

    /**
     * Atomically checks token and key consistency, validates partition ownership, then appends the
     * next offset while holding the coordinator lock.
     *
     * @param token current group member token
     * @param tp partition whose offset is committed
     * @param key group-and-partition offset key
     * @param nextOffset committed next offset
     * @param store durable offset store receiving the commit
     * @throws IllegalStateException if this coordinator is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for null {@code tp}, {@code
     *     key}, or {@code store}, or if the token or partition does not match the key, with {@link
     *     ErrorCode#ILLEGAL_GENERATION} for a stale token, with {@link ErrorCode#NOT_ASSIGNED} if
     *     the member does not own the partition, or with the error raised by the offset store
     * @throws ExerciseNotImplementedException while the delegated Step 20 validation method is a
     *     skeleton
     */
    public void commitOffset(
            GroupToken token,
            TopicPartition tp,
            OffsetKey key,
            long nextOffset,
            OffsetStore store) {
        lock.lock();
        try {
            ensureOpen();
            if (tp == null || key == null || store == null)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST,
                        "topic-partition, offset key, and store are required");
            if (token == null || !token.group().equals(key.group()) || !tp.equals(key.tp())) {
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "commit token and offset key do not match");
            }
            validate(token, tp);
            store.commit(key, nextOffset);
        } finally {
            lock.unlock();
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("group coordinator is closed");
    }

    /**
     * Marks this coordinator closed; repeated calls have no effect. Subsequent operations throw
     * {@link IllegalStateException}.
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            groups.clear();
        } finally {
            lock.unlock();
        }
    }

    private static final class GroupState {
        private final String topic;
        private final TreeMap<String, MemberState> members = new TreeMap<>();
        private int generation;
        private GroupAssignment assignment = new GroupAssignment(0, Map.of());

        private GroupState(String topic) {
            this.topic = topic;
        }
    }

    private static final class MemberState {
        private long lastHeartbeat;

        private MemberState(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }
    }
}
