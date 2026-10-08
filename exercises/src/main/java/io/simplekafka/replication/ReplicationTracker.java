package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.TimeSource;

import java.util.Objects;
import java.util.Set;

/** Steps 22–23: shared per-partition replica progress and acknowledgement state. */
public final class ReplicationTracker {
    private final ClusterAuthority authority;
    private final TopicPartition tp;
    private final int initialLeaderId;
    private final Set<Integer> initialReplicas;
    private final int minISR;
    private final TimeSource clock;
    private final long lagTimeoutMillis;
    private final PartitionState state;

    public ReplicationTracker(
            ClusterAuthority authority,
            TopicPartition tp,
            int leaderId,
            Set<Integer> replicas,
            int minISR,
            TimeSource clock,
            long lagTimeoutMillis) {
        this.authority = Objects.requireNonNull(authority);
        this.tp = Objects.requireNonNull(tp);
        this.initialLeaderId = leaderId;
        this.initialReplicas = Set.copyOf(replicas);
        this.minISR = minISR;
        this.clock = Objects.requireNonNull(clock);
        this.lagTimeoutMillis = lagTimeoutMillis;
        this.state = authority.partitionState(tp);
    }

    /**
     * Step 22: validate and publish one replica's current-epoch LEO. See Step22Test and book step
     * 22.
     */
    public void report(int brokerId, int epoch, long leo) {
        throw new ExerciseNotImplementedException(22, "ReplicationTracker.report");
    }

    /**
     * Step 22: remove ISR members that missed the configured catch-up deadline. See Step22Test and
     * book step 22.
     */
    public Set<Integer> expireLagging() {
        throw new ExerciseNotImplementedException(22, "ReplicationTracker.expireLagging");
    }

    /**
     * Step 22: advance the committed prefix from reported ISR progress. See Step22Test and book
     * step 22.
     */
    public long advanceHighWatermark() {
        throw new ExerciseNotImplementedException(22, "ReplicationTracker.advanceHighWatermark");
    }

    ClusterAuthority authority() {
        return authority;
    }

    TopicPartition tp() {
        return tp;
    }

    PartitionState state() {
        return state;
    }

    int configuredLeaderId() {
        return initialLeaderId;
    }

    Set<Integer> configuredReplicas() {
        return initialReplicas;
    }

    TimeSource clock() {
        return clock;
    }

    long lagTimeoutMillis() {
        return lagTimeoutMillis;
    }

    public int minISR() {
        return minISR;
    }

    public int epoch() {
        state.lock.lock();
        try {
            return state.epoch;
        } finally {
            state.lock.unlock();
        }
    }

    public int leaderId() {
        state.lock.lock();
        try {
            return state.leaderId;
        } finally {
            state.lock.unlock();
        }
    }

    public long highWatermark() {
        state.lock.lock();
        try {
            return state.highWatermark;
        } finally {
            state.lock.unlock();
        }
    }

    public ReplicationSnapshot snapshot() {
        return authority.snapshot(tp);
    }
}
