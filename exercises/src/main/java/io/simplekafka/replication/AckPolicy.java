package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Step 23: validates and waits for the configured produce acknowledgement level. */
public final class AckPolicy {
    private final ReplicationTracker tracker;

    /**
     * Creates an acknowledgement policy backed by the supplied replication tracker.
     *
     * @param tracker tracker supplying epoch, ISR, and high-watermark state
     * @throws NullPointerException if {@code tracker} is null
     */
    public AckPolicy(ReplicationTracker tracker) {
        this.tracker = Objects.requireNonNull(tracker);
    }

    /**
     * Step 23: allow {@link Acks#LEADER} immediately and reject {@link Acks#ALL} before append when
     * the ISR is below minISR. See Step23Test and book step 23.
     *
     * @param acks requested acknowledgement policy
     * @throws NullPointerException if {@code acks} is null
     * @throws io.simplekafka.CourseException with {@link
     *     io.simplekafka.ErrorCode#NOT_ENOUGH_REPLICAS} if ALL is requested while the ISR is below
     *     minISR
     * @throws ExerciseNotImplementedException while the Step 23 exercise method is a skeleton
     */
    public void validateBeforeAppend(Acks acks) {
        throw new ExerciseNotImplementedException(23, "AckPolicy.validateBeforeAppend");
    }

    /**
     * Step 23: wait for the captured epoch's high watermark until the absolute {@link
     * System#nanoTime()} deadline. LEADER returns immediately; ALL waits until HW reaches the
     * append boundary on the authority state's condition, which releases the state lock while
     * waiting. This method performs no RPC. See Step23Test and book step 23.
     *
     * @param result appended half-open offset range
     * @param acks requested acknowledgement policy
     * @param epoch epoch captured for this append
     * @param deadlineNanos absolute deadline in the {@link System#nanoTime()} time domain
     * @throws NullPointerException if {@code result} or {@code acks} is null
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#FENCED_EPOCH} for
     *     a stale epoch, with {@link io.simplekafka.ErrorCode#NOT_ENOUGH_REPLICAS} when ISR falls
     *     below minISR, or with {@link io.simplekafka.ErrorCode#REQUEST_TIMEOUT} at the deadline or
     *     after interruption; interruption restores the thread's interrupt flag and is retained as
     *     the cause
     * @throws ExerciseNotImplementedException while the Step 23 exercise method is a skeleton
     */
    public void await(AppendResult result, Acks acks, int epoch, long deadlineNanos) {
        throw new ExerciseNotImplementedException(23, "AckPolicy.await");
    }

    static long deadlineAfterMillis(long timeoutMillis) {
        long now = System.nanoTime();
        long nanos = TimeUnit.MILLISECONDS.toNanos(timeoutMillis);
        try {
            return Math.addExact(now, nanos);
        } catch (ArithmeticException overflow) {
            return Long.MAX_VALUE;
        }
    }
}
