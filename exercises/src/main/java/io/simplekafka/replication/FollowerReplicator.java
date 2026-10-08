package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ReplicaState;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.transport.RpcClient;

import java.util.Objects;

/** Step 21: a follower pulls records over the provided RPC connection. */
public final class FollowerReplicator {
    private final int brokerId;
    private final TopicPartition tp;
    private final PartitionLog log;
    private final RpcClient client;
    private final ReplicaState replicaState;

    /**
     * Creates a follower poller using a borrowed RPC client; this instance does not close the client.
     * The supplied broker ID is stored without checking it against the replica-state broker ID.
     *
     * @param brokerId identifier sent in replica-fetch requests
     * @param tp partition replicated by this poller
     * @param log local follower log to append to
     * @param client RPC client owned by the caller
     * @param replicaState local role, liveness, and epoch state
     * @throws NullPointerException if {@code tp}, {@code log}, {@code client}, or
     *     {@code replicaState} is null
     */
    public FollowerReplicator(
            int brokerId,
            TopicPartition tp,
            PartitionLog log,
            RpcClient client,
            ReplicaState replicaState) {
        this.brokerId = brokerId;
        this.tp = Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.client = Objects.requireNonNull(client);
        this.replicaState = Objects.requireNonNull(replicaState);
    }

    /**
     * Step 21: capture role and epoch under the replica-state monitor, perform the RPC outside it,
     * then recheck state and append under the monitor. An empty response appends nothing, and this
     * method does not report replica progress. See Step21Test and book step 21.
     *
     * @param maxRecords positive maximum records requested from the leader
     * @param maxBytes positive maximum encoded bytes requested from the leader
     * @return the number of records appended to the local log
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for nonpositive limits or an unexpected
     *     response body, with {@link io.simplekafka.ErrorCode#NOT_LEADER} if this replica is
     *     offline or already the leader, with {@link io.simplekafka.ErrorCode#FENCED_EPOCH} if its
     *     role or the response epoch is stale, or with
     *     {@link io.simplekafka.ErrorCode#CORRUPT_RECORD} if the leader LEO or returned offsets
     *     conflict with the local suffix; local log failures retain their storage error, and a
     *     remote failure retains its remote error code and message
     * @throws ExerciseNotImplementedException while the Step 21 exercise method is a skeleton
     */
    public int pollOnce(int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(21, "FollowerReplicator.pollOnce");
    }
}
