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

    public ReplicaAdmission(ClusterAuthority authority, PartitionLog localLog) {
        this.authority = Objects.requireNonNull(authority);
        this.localLog = Objects.requireNonNull(localLog);
    }

    /**
     * Step 28: admit only a fresh exact-LEO proof bound to this log identity and mutation version.
     * Validate the proof and publish the ISR transition while holding the local log monitor. See
     * Step28Test and book step 28.
     */
    public boolean tryAdd(int brokerId, TopicPartition tp, int epoch) {
        throw new ExerciseNotImplementedException(28, "ReplicaAdmission.tryAdd");
    }
}
