package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;

import java.util.Objects;

/** Step 25: elects only an online, clean replica from the previous ISR. */
public final class LeaderElection {
    private final ClusterAuthority authority;

    /**
     * Creates an election helper for the supplied cluster authority.
     *
     * @param authority authority containing broker liveness and partition replica state
     * @throws NullPointerException if {@code authority} is null
     */
    public LeaderElection(ClusterAuthority authority) {
        this.authority = Objects.requireNonNull(authority);
    }

    /**
     * Step 25: elect the lowest-ID online member of the old ISR whose reported LEO reaches the
     * high watermark. If the current leader is online it remains leader; if no candidate qualifies,
     * authority state is unchanged. A successful election advances the epoch, selects the candidate,
     * resets the ISR to that broker, resets other replicas' LEOs to zero and catch-up times to
     * {@code Long.MIN_VALUE}, and signals waiters. Concurrent epoch or leader changes cause the
     * election to retry. State changes are made under the partition lock without RPC. See Step25Test
     * and book step 25.
     *
     * @param tp partition to elect a leader for
     * @return metadata for the resulting leader and replica state
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#NO_ELIGIBLE_LEADER} if no clean online ISR candidate
     *     exists, or with {@link io.simplekafka.ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the
     *     authority has no state for the partition
     * @throws ExerciseNotImplementedException while the Step 25 exercise method is a skeleton
     */
    public PartitionMetadata elect(TopicPartition tp) {
        throw new ExerciseNotImplementedException(25, "LeaderElection.elect");
    }

    /**
     * Step 25: fence stale epochs and requests from nonleaders. Validation is performed against the
     * authority partition state without RPC. See Step25Test and book step 25.
     *
     * @param brokerId broker making the leader request
     * @param tp partition to validate
     * @param epoch epoch supplied with the request
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for a negative broker ID or epoch, with
     *     {@link io.simplekafka.ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if the authority has no
     *     partition state, with {@link io.simplekafka.ErrorCode#FENCED_EPOCH} for a stale epoch, or
     *     with {@link io.simplekafka.ErrorCode#NOT_LEADER} if the broker is not the online leader
     * @throws ExerciseNotImplementedException while the Step 25 exercise method is a skeleton
     */
    public void checkLeader(int brokerId, TopicPartition tp, int epoch) {
        throw new ExerciseNotImplementedException(25, "LeaderElection.checkLeader");
    }
}
