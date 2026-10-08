package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.RecoveryProof;
import io.simplekafka.cluster.RecoveryProofRegistry;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.transport.RpcClient;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;

/** Full-prefix clean repair used when a replica rejoins after an election. */
public final class ReplicaReconciler {
    private static final int FETCH_RECORDS = 4;
    private static final int FETCH_BYTES = 4_200_000;

    private final int brokerId;
    private final TopicPartition tp;
    private final PartitionLog log;
    private final RpcClient client;
    private final ClusterAuthority authority;

    public ReplicaReconciler(
            int brokerId,
            TopicPartition tp,
            PartitionLog log,
            RpcClient client,
            ClusterAuthority authority) {
        if (brokerId < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "broker id must be nonnegative");
        this.brokerId = brokerId;
        this.tp = Objects.requireNonNull(tp);
        this.log = Objects.requireNonNull(log);
        this.client = Objects.requireNonNull(client);
        this.authority = Objects.requireNonNull(authority);
    }

    /** Returns repaired LEO and installs proof bound to the exact leader LEO captured at entry. */
    public long reconcile(int expectedEpoch) {
        RecoveryProofRegistry.clear(authority, tp, brokerId);
        PartitionState state = authority.partitionState(tp);
        int leaderId;
        int capturedEpoch;
        long capturedHw;
        long capturedLeaderLeo;
        long localMutationVersion = log.mutationVersion();
        state.lock.lock();
        try {
            if (expectedEpoch != state.epoch)
                throw new CourseException(ErrorCode.FENCED_EPOCH, "reconciliation epoch is stale");
            if (brokerId == state.leaderId)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "leader cannot reconcile against itself");
            if (!state.assigned(brokerId))
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "broker is not an assigned replica");
            leaderId = state.leaderId;
            capturedEpoch = state.epoch;
            capturedHw = state.highWatermark;
            Long leo = state.reportedLEO.get(leaderId);
            if (leo == null)
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "leader progress is unavailable");
            capturedLeaderLeo = leo;
        } finally {
            state.lock.unlock();
        }

        List<LogRecord> leaderRecords = fetchCapturedPrefix(capturedEpoch, capturedLeaderLeo);
        long localStart = log.logStartOffset();
        long localLeo = log.logEndOffset();
        if (localStart != 0)
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD,
                    "full-prefix reconciliation requires log start offset 0");
        List<LogRecord> localRecords = readPrefix(localLeo);
        long mismatch = firstMismatch(localRecords, leaderRecords, localLeo, capturedLeaderLeo);
        boolean truncate = mismatch >= 0 || localLeo > capturedLeaderLeo;
        long divergence = mismatch >= 0 ? mismatch : Math.min(localLeo, capturedLeaderLeo);

        state.lock.lock();
        try {
            if (state.epoch != capturedEpoch || state.leaderId != leaderId)
                throw new CourseException(
                        ErrorCode.FENCED_EPOCH, "leader changed during reconciliation");
            synchronized (log) {
                if (log.logEndOffset() != localLeo
                        || log.logStartOffset() != localStart
                        || log.mutationVersion() != localMutationVersion)
                    throw new CourseException(
                            ErrorCode.REQUEST_TIMEOUT, "local log changed during reconciliation");
                if (state.highWatermark > capturedLeaderLeo)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "captured leader prefix no longer contains the committed high watermark");
                if (mismatch >= 0 && mismatch < capturedHw)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "replica diverges inside the captured committed prefix at offset "
                                    + mismatch);
                if (truncate && divergence < state.highWatermark)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "replica diverges inside the current committed prefix at offset "
                                    + divergence);

                if (truncate && divergence < localLeo) log.truncateTo(divergence);
                long appendFrom = truncate ? divergence : localLeo;
                if (appendFrom < capturedLeaderLeo) {
                    List<RecordData> suffix = new ArrayList<>();
                    for (long offset = appendFrom; offset < capturedLeaderLeo; offset++)
                        suffix.add(leaderRecords.get(Math.toIntExact(offset)).data());
                    log.append(suffix);
                }
                long repairedLeo = log.logEndOffset();
                if (repairedLeo != capturedLeaderLeo)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "reconciled LEO does not match captured leader LEO");
                RecoveryProof proof =
                        new RecoveryProof(
                                brokerId,
                                tp,
                                capturedEpoch,
                                capturedLeaderLeo,
                                repairedLeo,
                                capturedHw,
                                log,
                                log.mutationVersion());
                RecoveryProofRegistry.record(authority, proof);
                return repairedLeo;
            }
        } finally {
            state.lock.unlock();
        }
    }

    private List<LogRecord> fetchCapturedPrefix(int epoch, long leaderLeo) {
        List<LogRecord> records = new ArrayList<>();
        long offset = 0;
        while (offset < leaderLeo) {
            checkEpoch(epoch);
            Messages.Reply reply =
                    client.call(
                            new Messages.ReplicaFetchRequest(
                                    tp, brokerId, epoch, offset, FETCH_RECORDS, FETCH_BYTES, true));
            if (reply.error() != ErrorCode.NONE) {
                String message =
                        reply.body() instanceof Messages.ErrorBody error
                                ? error.message()
                                : reply.error().name();
                throw new CourseException(reply.error(), message);
            }
            if (!(reply.body() instanceof Messages.ReplicaFetchBody body))
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST,
                        "recovery fetch returned an unexpected response");
            if (body.epoch() != epoch)
                throw new CourseException(
                        ErrorCode.FENCED_EPOCH, "recovery response has a stale epoch");
            if (body.leaderLogEndOffset() < leaderLeo)
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "leader LEO moved behind the captured prefix");
            List<LogRecord> batch = body.records();
            if (batch.isEmpty())
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD,
                        "leader returned an empty recovery batch before captured LEO");
            for (LogRecord record : batch) {
                if (offset >= leaderLeo) break;
                if (record.offset() != offset)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "recovery response is not a contiguous captured prefix");
                records.add(record);
                offset++;
            }
            checkEpoch(epoch);
        }
        if (records.size() != leaderLeo)
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD,
                    "recovery scan did not cover the complete leader prefix");
        return records;
    }

    private void checkEpoch(int expectedEpoch) {
        PartitionState state = authority.partitionState(tp);
        state.lock.lock();
        try {
            if (state.epoch != expectedEpoch)
                throw new CourseException(
                        ErrorCode.FENCED_EPOCH, "leader epoch changed during recovery scan");
        } finally {
            state.lock.unlock();
        }
    }

    private List<LogRecord> readPrefix(long leo) {
        List<LogRecord> records = new ArrayList<>();
        long offset = 0;
        while (offset < leo) {
            List<LogRecord> batch = log.read(offset, FETCH_RECORDS, FETCH_BYTES);
            if (batch.isEmpty())
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "local log ended before its LEO");
            for (LogRecord record : batch) {
                if (record.offset() != offset || record.offset() >= leo)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD, "local log is not a contiguous prefix");
                records.add(record);
                offset++;
            }
        }
        return records;
    }

    private static long firstMismatch(
            List<LogRecord> local, List<LogRecord> leader, long localLeo, long leaderLeo) {
        long common = Math.min(localLeo, leaderLeo);
        for (long offset = 0; offset < common; offset++) {
            if (!sameRecord(
                    local.get(Math.toIntExact(offset)), leader.get(Math.toIntExact(offset))))
                return offset;
        }
        return -1;
    }

    private static boolean sameRecord(LogRecord left, LogRecord right) {
        if (left.offset() != right.offset()) return false;
        RecordData a = left.data();
        RecordData b = right.data();
        return a.timestamp() == b.timestamp()
                && Arrays.equals(a.key(), b.key())
                && Arrays.equals(a.value(), b.value());
    }

    /** The proof includes the exact leader LEO captured by the latest successful reconciliation. */
    public Optional<RecoveryProof> recoveryProof() {
        return RecoveryProofRegistry.find(authority, tp, brokerId);
    }
}
