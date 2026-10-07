package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.RecoveryProof;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.transport.RpcClient;
import java.util.Objects;
import java.util.Optional;

/** Step 26: compares a full local prefix with the selected leader before repairing a fork. */
public final class ReplicaReconciler {
    private final int brokerId;
    private final TopicPartition tp;
    private final PartitionLog log;
    private final RpcClient client;
    private final ClusterAuthority authority;

    public ReplicaReconciler(int brokerId, TopicPartition tp, PartitionLog log,
                             RpcClient client, ClusterAuthority authority) {
        this.brokerId = brokerId;
        this.tp = Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.client = Objects.requireNonNull(client);
        this.authority = Objects.requireNonNull(authority);
    }

    /** Step 26: capture log version before scanning, then check, repair and publish proof under authority-state and local-log monitors. See Step26Test and book step 26. */
    public long reconcile(int expectedEpoch) {
        throw new ExerciseNotImplementedException(26, "ReplicaReconciler.reconcile");
    }

    /** The proof binds the most recent successful repair to its exact captured leader LEO. */
    public Optional<RecoveryProof> recoveryProof() {
        return io.simplekafka.cluster.RecoveryProofRegistry.find(authority, tp, brokerId);
    }
}
