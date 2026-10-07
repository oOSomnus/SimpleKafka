package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.model.TopicPartition;
import java.util.Objects;

/** Step 28: admits only a current, online replica backed by a fresh recovery proof. */
public final class ReplicaAdmission {
    private final ClusterAuthority authority;

    public ReplicaAdmission(ClusterAuthority authority) { this.authority = Objects.requireNonNull(authority); }

    /** Step 28: admit only a replica with a fresh exact-LEO recovery proof. See Step28Test and book step 28. */
    public boolean tryAdd(int brokerId, TopicPartition tp, int epoch) {
        throw new ExerciseNotImplementedException(28, "ReplicaAdmission.tryAdd");
    }
}
