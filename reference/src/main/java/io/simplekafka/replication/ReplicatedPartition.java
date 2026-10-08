package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.broker.PartitionBackend;
import io.simplekafka.cluster.ClusterAuthority;
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

/** Leader-side partition operations backed by the local durable log and shared authority state. */
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
        Objects.requireNonNull(tp, "tp");
        this.log = Objects.requireNonNull(log, "log");
        this.replicaState = Objects.requireNonNull(replicaState, "replicaState");
        this.tracker = Objects.requireNonNull(tracker, "tracker");
        this.ackPolicy = Objects.requireNonNull(ackPolicy, "ackPolicy");
        this.state = tracker.state();
        if (!tp.equals(tracker.tp()))
            throw new IllegalArgumentException("tracker partition mismatch");
    }

    @Override
    public AppendResult produce(
            List<RecordData> records, Acks acks, int epoch, long timeoutMillis) {
        Objects.requireNonNull(records, "records");
        Objects.requireNonNull(acks, "acks");
        if (epoch < 0 || timeoutMillis < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "negative epoch or timeout");
        ackPolicy.validateBeforeAppend(acks);
        int brokerId = replicaState.brokerId();
        boolean online = tracker.authority().isOnline(brokerId);
        AppendResult result;
        state.lock.lock();
        try {
            syncRoleUnderLock(online);
            requireLeader(epoch, online);
            if (acks == Acks.ALL && state.isr.size() < tracker.minISR())
                throw new CourseException(
                        ErrorCode.NOT_ENOUGH_REPLICAS, "ISR is below minISR before append");
            result = log.append(records);
            // Keep authority progress and the physical append in the same partition critical
            // section.
            tracker.report(brokerId, epoch, result.nextOffset());
        } finally {
            state.lock.unlock();
        }
        ackPolicy.await(result, acks, epoch, AckPolicy.deadlineAfterMillis(timeoutMillis));
        return result;
    }

    @Override
    public List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch) {
        if (offset < 0 || maxRecords <= 0 || maxBytes <= 0 || epoch < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid fetch arguments");
        int brokerId = replicaState.brokerId();
        boolean online = tracker.authority().isOnline(brokerId);
        state.lock.lock();
        try {
            syncRoleUnderLock(online);
            requireLeader(epoch, online);
            long start = log.logStartOffset();
            long leo = log.logEndOffset();
            if (offset < start || offset > leo)
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE, "fetch offset is outside the local log");
            long hw = state.highWatermark;
            if (offset >= hw) return List.of();
            List<LogRecord> records = log.read(offset, maxRecords, maxBytes);
            if (records.isEmpty()) return records;
            int visible = 0;
            while (visible < records.size() && records.get(visible).offset() < hw) visible++;
            return visible == records.size() ? records : List.copyOf(records.subList(0, visible));
        } finally {
            state.lock.unlock();
        }
    }

    /** Handler hook for replica reads; unlike consumer fetch, it may read above the HW. */
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
        ClusterAuthority authority = tracker.authority();
        boolean requesterOnline = authority.isOnline(brokerId);
        boolean leaderOnline = authority.isOnline(replicaState.brokerId());
        if (!requesterOnline)
            throw new CourseException(ErrorCode.NOT_LEADER, "requesting replica is offline");

        // A follower reports progress only after it durably appends this reply.

        state.lock.lock();
        try {
            syncRoleUnderLock(leaderOnline);
            requireLeader(epoch, leaderOnline);
            if (!state.assigned(brokerId))
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "requester is not an assigned replica");
            long start = log.logStartOffset();
            long leo = log.logEndOffset();
            if (fetchOffset < start || fetchOffset > leo)
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE,
                        "replica fetch offset is outside leader log");
            List<LogRecord> records =
                    fetchOffset == leo ? List.of() : log.read(fetchOffset, maxRecords, maxBytes);
            return new Messages.ReplicaFetchBody(records, state.epoch, state.highWatermark, leo);
        } finally {
            state.lock.unlock();
        }
    }

    private void syncRoleUnderLock(boolean online) {
        replicaState.update(
                state.epoch,
                state.leaderId == replicaState.brokerId(),
                online && replicaState.isOnline());
    }

    private void requireLeader(int requestEpoch, boolean online) {
        if (requestEpoch != state.epoch)
            throw new CourseException(ErrorCode.FENCED_EPOCH, "request epoch is stale");
        if (state.leaderId != replicaState.brokerId()
                || !replicaState.isLeader()
                || !online
                || !replicaState.isOnline())
            throw new CourseException(
                    ErrorCode.NOT_LEADER, "broker is not the online partition leader");
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
