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

    /**
     * Attaches a tracker to the authority state for one partition and stores the supplied
     * configuration. The constructor does not validate that the supplied leader, replicas, minISR,
     * or lag timeout agree with that authority state.
     *
     * @param authority authority owning the partition state
     * @param tp partition tracked by this instance
     * @param leaderId configured initial leader identifier
     * @param replicas configured initial replica identifiers
     * @param minISR configured minimum in-sync replica count
     * @param clock clock used for lag timing
     * @param lagTimeoutMillis configured catch-up timeout in milliseconds
     * @throws NullPointerException if a required reference or a replica element is null
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the authority has no state
     *     for {@code tp}
     */
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
     * Step 22: validate and publish one replica's current-epoch LEO while holding the partition
     * state lock; no RPC is performed. A successful report stores progress, refreshes the caught-up
     * timestamp when the LEO reaches the leader LEO, advances the high watermark, and signals
     * waiters. See Step22Test and book step 22.
     *
     * @param brokerId reporting broker identifier
     * @param epoch leader epoch for the report
     * @param leo reported log end offset
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for negative, offline, or unassigned
     *     broker data, a follower LEO beyond the leader LEO, a leader LEO moving backward, or a
     *     follower LEO moving backward within the same epoch; with
     *     {@link io.simplekafka.ErrorCode#FENCED_EPOCH} for a stale epoch
     * @throws ExerciseNotImplementedException while the Step 22 exercise method is a skeleton
     */
    public void report(int brokerId, int epoch, long leo) {
        throw new ExerciseNotImplementedException(22, "ReplicationTracker.report");
    }

    /**
     * Step 22: remove non-leader ISR members with no caught-up timestamp or whose lag elapsed time
     * is at least {@code lagTimeoutMillis}; a zero timeout expires them immediately. When members
     * are removed, reconsider the high watermark and signal waiters. See Step22Test and book step 22.
     *
     * @return immutable set of removed non-leader broker identifiers
     * @throws ExerciseNotImplementedException while the Step 22 exercise method is a skeleton
     */
    public Set<Integer> expireLagging() {
        throw new ExerciseNotImplementedException(22, "ReplicationTracker.expireLagging");
    }

    /**
     * Step 22: advance the committed prefix to the minimum reported LEO of the ISR. The high
     * watermark is monotonic and does not advance when the ISR is below minISR or a member's LEO is
     * missing. See Step22Test and book step 22.
     *
     * @return the current high watermark
     * @throws ExerciseNotImplementedException while the Step 22 exercise method is a skeleton
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

    /**
     * Returns the configured minimum in-sync replica count.
     *
     * @return the minISR threshold
     */
    public int minISR() {
        return minISR;
    }

    /**
     * Returns the current leader epoch while holding the authority partition state lock.
     *
     * @return the current epoch
     */
    public int epoch() {
        state.lock.lock();
        try {
            return state.epoch;
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Returns the current leader identifier while holding the authority partition state lock.
     *
     * @return the current leader broker identifier
     */
    public int leaderId() {
        state.lock.lock();
        try {
            return state.leaderId;
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Returns the current high watermark while holding the authority partition state lock.
     *
     * @return the committed-prefix boundary
     */
    public long highWatermark() {
        state.lock.lock();
        try {
            return state.highWatermark;
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Returns an immutable point-in-time snapshot of the authority's replication state.
     *
     * @return the partition replication snapshot
     */
    public ReplicationSnapshot snapshot() {
        return authority.snapshot(tp);
    }
}
