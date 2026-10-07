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

    public FollowerReplicator(int brokerId, TopicPartition tp, PartitionLog log,
                              RpcClient client, ReplicaState replicaState) {
        this.brokerId = brokerId;
        this.tp = Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.client = Objects.requireNonNull(client);
        this.replicaState = Objects.requireNonNull(replicaState);
    }

    /** Step 21: fetch one bounded TCP batch and append its contiguous suffix. See Step21Test and book step 21. */
    public int pollOnce(int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(21, "FollowerReplicator.pollOnce");
    }
}
