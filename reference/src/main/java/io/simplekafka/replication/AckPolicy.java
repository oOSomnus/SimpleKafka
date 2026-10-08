package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;

import java.util.Objects;
import java.util.concurrent.TimeUnit;

/** Waits on the authority-owned condition without holding any network operation under the lock. */
public final class AckPolicy {
    private final ReplicationTracker tracker;

    public AckPolicy(ReplicationTracker tracker) {
        this.tracker = Objects.requireNonNull(tracker);
    }

    public void validateBeforeAppend(Acks acks) {
        Objects.requireNonNull(acks, "acks");
        if (acks == Acks.LEADER) return;
        PartitionState state = tracker.state();
        state.lock.lock();
        try {
            if (state.isr.size() < tracker.minISR())
                throw new CourseException(
                        ErrorCode.NOT_ENOUGH_REPLICAS, "ISR is below minISR before append");
        } finally {
            state.lock.unlock();
        }
    }

    public void await(AppendResult result, Acks acks, int epoch, long deadlineNanos) {
        Objects.requireNonNull(result, "result");
        Objects.requireNonNull(acks, "acks");
        if (acks == Acks.LEADER) return;
        PartitionState state = tracker.state();
        state.lock.lock();
        try {
            while (true) {
                if (epoch != state.epoch)
                    throw new CourseException(
                            ErrorCode.FENCED_EPOCH,
                            "produce acknowledgement belongs to a stale epoch");
                if (state.isr.size() < tracker.minISR())
                    throw new CourseException(
                            ErrorCode.NOT_ENOUGH_REPLICAS,
                            "ISR fell below minISR while awaiting acknowledgement");
                if (state.highWatermark >= result.nextOffset()) return;
                long remaining = deadlineNanos - System.nanoTime();
                if (remaining <= 0)
                    throw new CourseException(
                            ErrorCode.REQUEST_TIMEOUT, "timed out waiting for high watermark");
                try {
                    state.changed.awaitNanos(remaining);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new CourseException(
                            ErrorCode.REQUEST_TIMEOUT,
                            "interrupted while awaiting high watermark",
                            exception);
                }
            }
        } finally {
            state.lock.unlock();
        }
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
