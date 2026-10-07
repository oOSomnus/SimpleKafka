package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ReplicaState;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.transport.RpcClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Pulls follower records from the current leader through the real framed TCP protocol. */
public final class FollowerReplicator {
    private final int brokerId;
    private final TopicPartition tp;
    private final PartitionLog log;
    private final RpcClient client;
    private final ReplicaState replicaState;

    public FollowerReplicator(int brokerId, TopicPartition tp, PartitionLog log,
                              RpcClient client, ReplicaState replicaState) {
        if (brokerId < 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "broker id must be nonnegative");
        this.brokerId = brokerId;
        this.tp = Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.client = Objects.requireNonNull(client);
        this.replicaState = Objects.requireNonNull(replicaState);
        if (replicaState.brokerId() != brokerId) throw new IllegalArgumentException("replica state broker mismatch");
    }

    /** Fetches and durably appends one bounded batch; it never reports progress itself. */
    public int pollOnce(int maxRecords, int maxBytes) {
        if (maxRecords <= 0 || maxBytes <= 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "replication limits must be positive");
        int epoch;
        synchronized (replicaState) {
            if (!replicaState.isOnline()) throw new CourseException(ErrorCode.NOT_LEADER, "follower broker is offline");
            if (replicaState.isLeader()) throw new CourseException(ErrorCode.NOT_LEADER, "leader cannot run follower replication");
            epoch = replicaState.epoch();
        }

        long localLeo = log.logEndOffset();
        Messages.Reply reply = client.call(new Messages.ReplicaFetchRequest(tp, brokerId, epoch,
                localLeo, maxRecords, maxBytes, false));
        if (reply.error() != ErrorCode.NONE) throw remoteFailure(reply);
        if (!(reply.body() instanceof Messages.ReplicaFetchBody body))
            throw new CourseException(ErrorCode.INVALID_REQUEST, "replica fetch returned an unexpected response");
        if (body.epoch() != epoch) throw new CourseException(ErrorCode.FENCED_EPOCH, "leader returned a different epoch");
        if (body.leaderLogEndOffset() < localLeo)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "follower log extends beyond leader; reconciliation is required");
        List<LogRecord> records = body.records();
        long expected = localLeo;
        List<io.simplekafka.model.RecordData> data = new ArrayList<>(records.size());
        for (LogRecord record : records) {
            if (record.offset() != expected || record.offset() >= body.leaderLogEndOffset())
                throw new CourseException(ErrorCode.CORRUPT_RECORD, "replica fetch is not a contiguous suffix");
            data.add(record.data());
            expected++;
        }

        synchronized (replicaState) {
            if (replicaState.epoch() != epoch)
                throw new CourseException(ErrorCode.FENCED_EPOCH, "replica epoch changed during fetch");
            if (!replicaState.isOnline())
                throw new CourseException(ErrorCode.NOT_LEADER, "follower broker is offline");
            if (replicaState.isLeader())
                throw new CourseException(ErrorCode.NOT_LEADER, "leader cannot run follower replication");
            if (data.isEmpty()) return 0;
            AppendResult appended = log.append(data);
            if (appended.firstOffset() != localLeo || appended.nextOffset() != expected)
                throw new CourseException(ErrorCode.CORRUPT_RECORD, "follower append offset diverged from leader");
            return data.size();
        }
    }


    private static CourseException remoteFailure(Messages.Reply reply) {
        String message = reply.body() instanceof Messages.ErrorBody error ? error.message() : reply.error().name();
        return new CourseException(reply.error(), message);
    }
}
