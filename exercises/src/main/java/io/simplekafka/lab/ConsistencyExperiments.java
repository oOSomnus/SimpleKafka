package io.simplekafka.lab;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;

/** Student algorithms for the Step 29 cross-layer fault experiments. */
public final class ConsistencyExperiments {
    private ConsistencyExperiments() {}

    /** Runs an outcome-unknown producer experiment through a one-shot reply-dropping proxy. */
    public static ConsistencyTrace producerOutcomeUnknown(
            ClusterHarness cluster, TopicPartition tp, RecordData data, FaultProxy proxy) {
        throw new ExerciseNotImplementedException(
                29, "ConsistencyExperiments.producerOutcomeUnknown");
    }

    /** Runs a leader-acknowledged uncommitted-tail election experiment. */
    public static ConsistencyTrace leaderAckLoss(
            ClusterHarness cluster, TopicPartition tp, RecordData data) {
        throw new ExerciseNotImplementedException(29, "ConsistencyExperiments.leaderAckLoss");
    }

    /** Runs an ALL-acknowledgment timeout followed by late replication. */
    public static ConsistencyTrace allAckTimeout(
            ClusterHarness cluster, TopicPartition tp, RecordData data) {
        throw new ExerciseNotImplementedException(29, "ConsistencyExperiments.allAckTimeout");
    }

    /** Runs a side-effect replay experiment after the consumer commit request is rejected. */
    public static ConsistencyTrace consumerReplay(
            ClusterHarness cluster,
            TopicPartition tp,
            String group,
            RecordData data,
            EffectRecorder effects,
            FaultProxy proxy)
            throws Exception {
        throw new ExerciseNotImplementedException(29, "ConsistencyExperiments.consumerReplay");
    }

    /** Runs a stale group-generation experiment after the old member's callback has occurred. */
    public static ConsistencyTrace staleGroupSideEffect(
            ClusterHarness cluster,
            TopicPartition tp,
            String group,
            RecordData data,
            EffectRecorder effects) {
        throw new ExerciseNotImplementedException(
                29, "ConsistencyExperiments.staleGroupSideEffect");
    }
}
