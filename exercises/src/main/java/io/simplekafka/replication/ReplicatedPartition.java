package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.broker.PartitionBackend;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.ReplicaState;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.protocol.Messages;
import io.simplekafka.storage.PartitionLog;

import java.util.List;
import java.util.Objects;

/** Step 24: connects the local log, replica progress, and acknowledgement policy. */
public final class ReplicatedPartition implements PartitionBackend, ReplicaFetchBackend {
    private final PartitionLog log;
    private final ReplicaState replicaState;
    private final ReplicationTracker tracker;
    private final AckPolicy ackPolicy;
    private final PartitionState state;

    public ReplicatedPartition(
            io.simplekafka.model.TopicPartition tp,
            PartitionLog log,
            ReplicaState replicaState,
            ReplicationTracker tracker,
            AckPolicy ackPolicy) {
        Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.replicaState = Objects.requireNonNull(replicaState);
        this.tracker = Objects.requireNonNull(tracker);
        this.ackPolicy = Objects.requireNonNull(ackPolicy);
        this.state = tracker.state();
        if (!tp.equals(tracker.tp()))
            throw new IllegalArgumentException("tracker partition mismatch");
    }

    /**
     * Step 24: append on the current leader and apply the selected acknowledgement policy. See
     * Step24Test and book step 24.
     */
    @Override
    public AppendResult produce(
            List<RecordData> records, Acks acks, int epoch, long timeoutMillis) {
        throw new ExerciseNotImplementedException(24, "ReplicatedPartition.produce");
    }

    /** Step 24: serve only the current committed prefix. See Step24Test and book step 24. */
    @Override
    public List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch) {
        throw new ExerciseNotImplementedException(24, "ReplicatedPartition.fetch");
    }

    /** Provided handler hook: replica reads expose leader LEO rather than truncating at HW. */
    @Override
    public Messages.ReplicaFetchBody fetchForReplica(
            int brokerId,
            int epoch,
            long fetchOffset,
            int maxRecords,
            int maxBytes,
            boolean recoveryRead) {
        if (brokerId < 0 || epoch < 0 || fetchOffset < 0 || maxRecords <= 0 || maxBytes <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid replica fetch arguments");
        io.simplekafka.cluster.ClusterAuthority authority = tracker.authority();
        if (!authority.isOnline(brokerId))
            throw new CourseException(ErrorCode.NOT_LEADER, "replica broker is offline");
        state.lock.lock();
        try {
            if (epoch != state.epoch)
                throw new CourseException(ErrorCode.FENCED_EPOCH, "replica fetch epoch is stale");
            if (state.leaderId != replicaState.brokerId() || !replicaState.isLeader())
                throw new CourseException(
                        ErrorCode.NOT_LEADER, "replica fetch must be served by the current leader");
            if (!state.assigned(brokerId))
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "requester is not an assigned replica");
            long leo = log.logEndOffset();
            if (fetchOffset > leo)
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE, "replica fetch offset exceeds leader LEO");
            if (fetchOffset < log.logStartOffset())
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE, "replica fetch offset precedes log start");
            List<LogRecord> records =
                    fetchOffset == leo ? List.of() : log.read(fetchOffset, maxRecords, maxBytes);
            return new Messages.ReplicaFetchBody(records, state.epoch, state.highWatermark, leo);
        } finally {
            state.lock.unlock();
        }
    }

    @Override
    public long logStartOffset() {
        return log.logStartOffset();
    }

    @Override
    public long logEndOffset() {
        return log.logEndOffset();
    }

    @Override
    public long highWatermark() {
        return tracker.highWatermark();
    }

    @Override
    public int epoch() {
        return tracker.epoch();
    }
}
