package io.simplekafka.cluster;

import io.simplekafka.model.TopicPartition;
import java.util.Objects;

/** Evidence that one replica's complete prefix was reconciled against a captured leader log. */
public record RecoveryProof(int brokerId, TopicPartition tp, int epoch, long leaderLEO,
                            long localLEO, long highWatermark) {
    public RecoveryProof {
        if (brokerId < 0 || epoch < 0 || leaderLEO < 0 || localLEO < 0 || highWatermark < 0)
            throw new IllegalArgumentException("recovery proof fields must be nonnegative");
        Objects.requireNonNull(tp, "tp");
    }
}
