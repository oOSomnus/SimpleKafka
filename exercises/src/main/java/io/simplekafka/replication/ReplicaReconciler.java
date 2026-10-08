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

    /**
     * Creates a reconciler for one broker's local replica and a borrowed RPC client. The supplied
     * broker ID is stored without range validation; this instance does not close the client.
     *
     * @param brokerId local replica broker identifier
     * @param tp partition to reconcile
     * @param log local replica log to repair
     * @param client RPC client owned by the caller
     * @param authority authority containing the current leader and replica state
     * @throws NullPointerException if {@code tp}, {@code log}, {@code client}, or
     *     {@code authority} is null
     */
    public ReplicaReconciler(
            int brokerId,
            TopicPartition tp,
            PartitionLog log,
            RpcClient client,
            ClusterAuthority authority) {
        this.brokerId = brokerId;
        this.tp = Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.client = Objects.requireNonNull(client);
        this.authority = Objects.requireNonNull(authority);
    }

    /**
     * Step 26: clear any prior proof, capture epoch, high watermark, leader LEO, and local mutation
     * version, then compare the complete prefix using recovery-read batches of four records and
     * 4.2 MB. The local log must start at zero; a conflict at or below the captured/current high
     * watermark is not repairable. Leader RPCs run outside locks. Before repair, revalidate the
     * captured epoch, leader, LEO, log start, and mutation version while holding the authority state
     * lock before the local log monitor; then truncate the divergence and append the leader suffix.
     * A failed reconciliation publishes no proof. See Step26Test and book step 26.
     *
     * @param expectedEpoch epoch the caller expects the partition to use
     * @return the repaired local log end offset, matching the captured leader LEO
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the authority has no state
     *     for this partition, with {@link io.simplekafka.ErrorCode#FENCED_EPOCH} for a stale epoch,
     *     with {@link io.simplekafka.ErrorCode#INVALID_REQUEST} if this broker is the leader or is
     *     unassigned, with {@link io.simplekafka.ErrorCode#CORRUPT_RECORD} for an unavailable or
     *     unrepairable leader prefix, with {@link io.simplekafka.ErrorCode#REQUEST_TIMEOUT} if the
     *     local log changes before repair completes, or with
     *     {@link io.simplekafka.ErrorCode#STORAGE_ERROR} if local storage fails, or with the error
     *     returned by a replica fetch
     * @throws ExerciseNotImplementedException while the Step 26 exercise method is a skeleton
     */
    public long reconcile(int expectedEpoch) {
        throw new ExerciseNotImplementedException(26, "ReplicaReconciler.reconcile");
    }

    /**
     * Returns the current recovery proof registered for this authority, partition, and broker.
     * A new reconciliation or failed admission clears the prior proof.
     *
     * @return the proof from the most recent successful reconciliation, or empty when none is
     *     registered
     */
    public Optional<RecoveryProof> recoveryProof() {
        return io.simplekafka.cluster.RecoveryProofRegistry.find(authority, tp, brokerId);
    }
}
