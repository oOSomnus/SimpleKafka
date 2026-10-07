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
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.locks.ReentrantLock;

/** Single-process, same-topic group membership, assignment, heartbeat, and generation fence. */
public final class GroupCoordinator implements AutoCloseable {
    private final PartitionCatalog catalog;
    private final RoundRobinAssignor assignor;
    private final TimeSource time;
    private final long sessionTimeoutMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private boolean closed;

    public GroupCoordinator(PartitionCatalog catalog, RoundRobinAssignor assignor,
                            TimeSource time, long sessionTimeoutMillis) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.assignor = Objects.requireNonNull(assignor, "assignor");
        this.time = Objects.requireNonNull(time, "time");
        if (sessionTimeoutMillis <= 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "sessionTimeoutMillis must be positive");
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }

    /**
     * Step 18: add or refresh a same-topic member and immediately rebalance the group.
     * Contract: a new member increments generation; idempotent join only refreshes heartbeat.
     * Test: Step18Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 18.
     */
    public GroupAssignment join(String group, String member, String topic) {
        throw new ExerciseNotImplementedException(18, "join");
    }

    /**
     * Step 18: remove a known member and rebalance every remaining member.
     * Contract: unknown members fail; empty groups retain their generation and committed offsets.
     * Test: Step18Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 18.
     */
    public GroupAssignment leave(String group, String member) {
        throw new ExerciseNotImplementedException(18, "leave");
    }

    /**
     * Step 18: return the current full-group assignment for a known member.
     * Contract: unknown members return UNKNOWN_MEMBER. Test: Step18Test.
     * Lesson: docs/book/chapters/05-consumer-groups.tex, Step 18.
     */
    public GroupAssignment assignment(String group, String member) {
        throw new ExerciseNotImplementedException(18, "assignment");
    }

    /**
     * Step 19: validate membership/generation then record a heartbeat at the injected time.
     * Contract: stale or unknown tokens do not refresh the member. Test: Step19Test.
     * Lesson: docs/book/chapters/05-consumer-groups.tex, Step 19.
     */
    public void heartbeat(GroupToken token) {
        throw new ExerciseNotImplementedException(19, "heartbeat");
    }

    /**
     * Step 19: expire all due members, once per group generation, and return sorted group/member ids.
     * Contract: expiry occurs at elapsed time >= sessionTimeoutMillis. Test: Step19Test.
     * Lesson: docs/book/chapters/05-consumer-groups.tex, Step 19.
     */
    public Set<String> expire() {
        throw new ExerciseNotImplementedException(19, "expire");
    }

    /**
     * Step 20: fence a token by current generation and verify the member owns the partition.
     * Contract: stale token -> ILLEGAL_GENERATION; a current non-owner -> NOT_ASSIGNED.
     * Test: Step20Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 20.
     */
    public void validate(GroupToken token, TopicPartition tp) {
        throw new ExerciseNotImplementedException(20, "validate");
    }

    /** Atomically fences a group commit with validation and the offset-store append. */
    public void commitOffset(GroupToken token, TopicPartition tp, OffsetKey key,
                             long nextOffset, OffsetStore store) {
        lock.lock();
        try {
            ensureOpen();
            Objects.requireNonNull(key, "key");
            Objects.requireNonNull(store, "store");
            if (token == null || tp == null || !token.group().equals(key.group()) || !tp.equals(key.tp())) {
                throw new CourseException(ErrorCode.INVALID_REQUEST, "commit token and offset key do not match");
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

    @Override public void close() {
        lock.lock();
        try {
            closed = true;
        } finally {
            lock.unlock();
        }
    }
}
