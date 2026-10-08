package io.simplekafka.cluster;

import io.simplekafka.model.TopicPartition;
import io.simplekafka.storage.PartitionLog;

import java.util.Objects;

/**
 * In-JVM evidence that one replica's complete prefix was reconciled against a captured leader log.
 *
 * <p>Admission matches this proof to the exact local log object and captured mutation version;
 * a later local mutation makes the proof stale.
 *
 * @param brokerId broker that owns the reconciled replica
 * @param tp partition whose prefix was reconciled
 * @param epoch leader epoch used for reconciliation
 * @param leaderLEO leader log-end offset captured during reconciliation
 * @param localLEO local log-end offset after reconciliation
 * @param highWatermark high watermark captured with the reconciliation state
 * @param localLog exact local log object that was reconciled
 * @param localMutationVersion local log mutation version captured in this proof
 */
public record RecoveryProof(
        int brokerId,
        TopicPartition tp,
        int epoch,
        long leaderLEO,
        long localLEO,
        long highWatermark,
        PartitionLog localLog,
        long localMutationVersion) {
    /**
     * Creates a proof after validating its offsets, epoch, and required references.
     *
     * @param brokerId broker that owns the reconciled replica
     * @param tp partition whose prefix was reconciled
     * @param epoch leader epoch used for reconciliation
     * @param leaderLEO leader log-end offset captured during reconciliation
     * @param localLEO local log-end offset after reconciliation
     * @param highWatermark high watermark captured with the reconciliation state
     * @param localLog exact local log object that was reconciled
     * @param localMutationVersion local log mutation version captured in this proof
     * @throws IllegalArgumentException if a numeric proof value is negative
     * @throws NullPointerException if {@code tp} or {@code localLog} is {@code null}
     */
    public RecoveryProof {
        if (brokerId < 0
                || epoch < 0
                || leaderLEO < 0
                || localLEO < 0
                || highWatermark < 0
                || localMutationVersion < 0)
            throw new IllegalArgumentException("recovery proof fields must be nonnegative");
        Objects.requireNonNull(tp, "tp");
        Objects.requireNonNull(localLog, "localLog");
    }
}
