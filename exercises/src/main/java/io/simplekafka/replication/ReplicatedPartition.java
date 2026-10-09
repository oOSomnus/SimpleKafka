package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.broker.IdempotentProduceBackend;
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

/** Step 24: connects the local log, replica progress, and acknowledgement policy. */
public final class ReplicatedPartition
        implements PartitionBackend, ReplicaFetchBackend, IdempotentProduceBackend {
    private final PartitionLog log;
    private final ReplicaState replicaState;
    private final ReplicationTracker tracker;
    private final AckPolicy ackPolicy;
    private final PartitionState state;
    private final IdempotentAppender idempotentAppender;

    /**
     * Creates a backend for the supplied partition and replication components.
     *
     * @param tp partition represented by this backend
     * @param log local partition log
     * @param replicaState local broker role and liveness state
     * @param tracker authority-backed replication progress for the partition
     * @param ackPolicy acknowledgement policy for producer requests
     * @throws NullPointerException if any argument is null
     * @throws IllegalArgumentException if {@code tp} differs from the tracker's partition
     */
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
        this.idempotentAppender = new IdempotentAppender(log);
        this.state = tracker.state();
        if (!tp.equals(tracker.tp()))
            throw new IllegalArgumentException("tracker partition mismatch");
    }

    /**
     * Step 24: append on the current leader and apply the selected acknowledgement policy; ALL is
     * rejected before append when the ISR is below minISR. The authority state lock precedes the
     * local log monitor; the acknowledgement wait runs after the lock is released, and no RPC is
     * performed while either is held. See Step24Test and book step 24.
     *
     * @param records records to append
     * @param acks requested acknowledgement policy
     * @param epoch leader epoch supplied with the request
     * @param timeoutMillis nonnegative acknowledgement timeout in milliseconds
     * @return the appended half-open offset range after the requested acknowledgement condition is
     *     met
     * @throws NullPointerException if {@code records} or {@code acks} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for a negative epoch or
     *     timeout, with {@link ErrorCode#FENCED_EPOCH} for a stale epoch, with {@link
     *     ErrorCode#NOT_LEADER} if this broker is not the online leader, with {@link
     *     ErrorCode#NOT_ENOUGH_REPLICAS} when ALL is requested below minISR, with {@link
     *     ErrorCode#REQUEST_TIMEOUT} if the acknowledgement deadline expires, or with {@link
     *     ErrorCode#STORAGE_ERROR} if the local log fails; the append may already have occurred
     *     when acknowledgement fails
     * @throws ExerciseNotImplementedException while the Step 24 exercise method is a skeleton
     */
    @Override
    public AppendResult produce(
            List<RecordData> records, Acks acks, int epoch, long timeoutMillis) {
        throw new ExerciseNotImplementedException(24, "ReplicatedPartition.produce");
    }

    /**
     * Step 30: append or resolve a durable idempotent batch and then apply the original ACK
     * condition. Validate leader/minISR on every duplicate; do not hold either lock while waiting.
     *
     * @throws ExerciseNotImplementedException while the Step 30 exercise method is a skeleton
     */
    @Override
    public AppendResult produceIdempotent(
            List<RecordData> records,
            Acks acks,
            int leaderEpoch,
            long timeoutMillis,
            long producerId,
            int producerEpoch,
            long firstSequence) {
        throw new ExerciseNotImplementedException(30, "ReplicatedPartition.produceIdempotent");
    }

    /**
     * Step 24: serve only the current committed prefix. An offset at or above the high watermark
     * and no greater than the log end returns no records. The authority state lock precedes the
     * local log monitor, and no RPC is performed while they are held. See Step24Test and book step
     * 24.
     *
     * @param offset first requested offset
     * @param maxRecords positive record limit
     * @param maxBytes positive encoded-byte limit
     * @param epoch leader epoch supplied with the request
     * @return records from the committed prefix, never including offsets at or above the high
     *     watermark
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for a negative offset or
     *     epoch, or nonpositive limits; with {@link ErrorCode#FENCED_EPOCH} for a stale epoch, with
     *     {@link ErrorCode#NOT_LEADER} if this broker is not the online leader, with {@link
     *     ErrorCode#OFFSET_OUT_OF_RANGE} if {@code offset} is outside {@code [logStartOffset(),
     *     logEndOffset()]}, or with {@link ErrorCode#STORAGE_ERROR} if reading the local log fails
     * @throws ExerciseNotImplementedException while the Step 24 exercise method is a skeleton
     */
    @Override
    public List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch) {
        throw new ExerciseNotImplementedException(24, "ReplicatedPartition.fetch");
    }

    /**
     * Serves assigned replicas from the leader log, including records at or above the high
     * watermark; fetching at LEO returns an empty record list. It captures requester and leader
     * liveness outside the partition-state lock, then refreshes this broker's role/online mirror
     * and fences the request under that lock. {@code recoveryRead} is ignored, and this read
     * changes no ISR, high watermark, or replica progress. The authority state lock is held while
     * reading the local log.
     *
     * @param brokerId requesting replica broker identifier
     * @param epoch leader epoch supplied by the requester
     * @param fetchOffset first requested leader offset
     * @param maxRecords positive record limit
     * @param maxBytes positive encoded-byte limit
     * @param recoveryRead recovery-read flag, currently ignored by this implementation
     * @return replica-fetch response containing records, current epoch, high watermark, and leader
     *     LEO
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for invalid arguments or an
     *     unassigned requester, with {@link ErrorCode#NOT_LEADER} if the requester is offline or
     *     this broker is not the online partition leader, with {@link ErrorCode#FENCED_EPOCH} for a
     *     stale epoch, with {@link ErrorCode#OFFSET_OUT_OF_RANGE} if {@code fetchOffset} is outside
     *     {@code [logStartOffset(), logEndOffset()]}, or with {@link ErrorCode#STORAGE_ERROR} if
     *     reading the local log fails
     */
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

    /**
     * Returns the local log's first retained offset.
     *
     * @return the local log start offset
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if the local log is closed
     */
    @Override
    public long logStartOffset() {
        return log.logStartOffset();
    }

    /**
     * Returns the local log's next offset after its final record.
     *
     * @return the local log end offset
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if the local log is closed
     */
    @Override
    public long logEndOffset() {
        return log.logEndOffset();
    }

    /**
     * Returns the tracker's current high watermark.
     *
     * @return the committed-prefix boundary
     */
    @Override
    public long highWatermark() {
        return tracker.highWatermark();
    }

    /**
     * Returns the tracker's current leader epoch.
     *
     * @return the current epoch
     */
    @Override
    public int epoch() {
        return tracker.epoch();
    }
}
