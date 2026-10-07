package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import java.util.Objects;

/** Step 25: elects only an online, clean replica from the previous ISR. */
public final class LeaderElection {
    private final ClusterAuthority authority;

    public LeaderElection(ClusterAuthority authority) { this.authority = Objects.requireNonNull(authority); }

    /** Step 25: elect the lowest eligible online member of the old ISR. See Step25Test and book step 25. */
    public PartitionMetadata elect(TopicPartition tp) {
        throw new ExerciseNotImplementedException(25, "LeaderElection.elect");
    }

    /** Step 25: fence stale epochs and nonleaders. See Step25Test and book step 25. */
    public void checkLeader(int brokerId, TopicPartition tp, int epoch) {
        throw new ExerciseNotImplementedException(25, "LeaderElection.checkLeader");
    }
}
