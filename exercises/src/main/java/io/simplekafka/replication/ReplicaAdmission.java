package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.storage.PartitionLog;

import java.util.Objects;

/** Step 28: admits only a current, online replica backed by a fresh recovery proof. */
public final class ReplicaAdmission {
    private final ClusterAuthority authority;
    private final PartitionLog localLog;

    /**
     * Creates an admission helper for the supplied authority and local replica log.
     *
     * @param authority authority containing the partition's replica state
     * @param localLog local log whose recovery proof is to be checked
     * @throws NullPointerException if {@code authority} or {@code localLog} is null
     */
    public ReplicaAdmission(ClusterAuthority authority, PartitionLog localLog) {
        this.authority = Objects.requireNonNull(authority);
        this.localLog = Objects.requireNonNull(localLog);
    }

    /**
     * Step 28: admit only a fresh proof bound to this broker, partition, and epoch whose {@code
     * leaderLEO == localLEO == current leader LEO}, with high watermark no greater than leader LEO,
     * the same log identity and mutation version, log start zero, and log end equal to leader LEO.
     * Under the authority state lock followed by the local log monitor, success adds the replica to
     * the ISR, updates its caught-up time, advances the high watermark, signals waiters, and clears
     * the proof; no RPC is performed. Stale/offline/leader requests and proof mismatches return
     * false, with mismatches clearing the proof. See Step28Test and book step 28.
     *
     * @param brokerId replica broker requesting admission
     * @param tp partition whose ISR is updated
     * @param epoch current leader epoch expected by the requester
     * @return true if the replica is already in the ISR or is admitted; false if current state or
     *     the recovery proof does not permit admission
     * @throws io.simplekafka.CourseException with {@link
     *     io.simplekafka.ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the authority has no state for
     *     {@code tp}, or with {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for negative
     *     arguments or an unassigned broker, or with {@link io.simplekafka.ErrorCode#STORAGE_ERROR}
     *     if local log access fails while checking the recovery proof
     * @throws ExerciseNotImplementedException while the Step 28 exercise method is a skeleton
     */
    public boolean tryAdd(int brokerId, TopicPartition tp, int epoch) {
        throw new ExerciseNotImplementedException(28, "ReplicaAdmission.tryAdd");
    }
}
