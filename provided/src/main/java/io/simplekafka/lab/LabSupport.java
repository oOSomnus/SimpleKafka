package io.simplekafka.lab;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;

import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.TimeUnit;

/** Shared observation and deterministic replication operations for consistency experiments. */
public final class LabSupport {
    private static final long OPERATION_TIMEOUT_NANOS = TimeUnit.SECONDS.toNanos(5);
    private static final int RPC_TIMEOUT_MILLIS = 5_000;
    private static final int OBSERVATION_MAX_RECORDS = 10;

    private LabSupport() {}

    /** Replicates each online follower through TCP, then reports only its durable physical LEO. */
    public static void replicateAndReport(ClusterHarness cluster, TopicPartition tp) {
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(tp, "tp");
        long deadline = System.nanoTime() + OPERATION_TIMEOUT_NANOS;
        var initial = cluster.authority().metadata(tp);
        int leaderId = initial.leaderId();
        int epoch = initial.epoch();

        while (true) {
            for (int brokerId : ClusterHarness.BROKER_IDS) {
                if (brokerId == leaderId || !cluster.authority().isOnline(brokerId)) continue;
                while (true) {
                    ensureSameLeader(cluster, tp, leaderId, epoch);
                    long leaderLeo = cluster.partitionLog(leaderId, tp).logEndOffset();
                    long followerLeo = cluster.partitionLog(brokerId, tp).logEndOffset();
                    if (followerLeo > leaderLeo)
                        throw new CourseException(
                                ErrorCode.CORRUPT_RECORD,
                                "follower log extends beyond the current leader");
                    if (followerLeo == leaderLeo) break;
                    if (System.nanoTime() >= deadline)
                        throw new CourseException(
                                ErrorCode.REQUEST_TIMEOUT, "follower replication deadline expired");
                    int copied =
                            cluster.replicateOnce(
                                    brokerId,
                                    tp,
                                    OBSERVATION_MAX_RECORDS,
                                    MessageCodec.MAX_REPLICA_FETCH_BYTE_BUDGET);
                    long afterLeo = cluster.partitionLog(brokerId, tp).logEndOffset();
                    long currentLeaderLeo = cluster.partitionLog(leaderId, tp).logEndOffset();
                    ensureSameLeader(cluster, tp, leaderId, epoch);
                    if (afterLeo > currentLeaderLeo)
                        throw new CourseException(
                                ErrorCode.CORRUPT_RECORD,
                                "follower log extends beyond the current leader");
                    if (afterLeo < currentLeaderLeo && copied == 0)
                        throw new CourseException(
                                ErrorCode.REQUEST_TIMEOUT,
                                "replication made no progress while the follower was behind");
                }
                ensureSameLeader(cluster, tp, leaderId, epoch);
                long localLeo = cluster.partitionLog(brokerId, tp).logEndOffset();
                long leaderLeo = cluster.partitionLog(leaderId, tp).logEndOffset();
                if (localLeo == leaderLeo) cluster.tracker(tp).report(brokerId, epoch, localLeo);
            }

            ensureSameLeader(cluster, tp, leaderId, epoch);
            long leaderLeo = cluster.partitionLog(leaderId, tp).logEndOffset();
            boolean converged = true;
            for (int brokerId : ClusterHarness.BROKER_IDS) {
                if (brokerId != leaderId
                        && cluster.authority().isOnline(brokerId)
                        && cluster.partitionLog(brokerId, tp).logEndOffset() != leaderLeo) {
                    converged = false;
                    break;
                }
            }
            if (converged) return;
            if (System.nanoTime() >= deadline)
                throw new CourseException(
                        ErrorCode.REQUEST_TIMEOUT,
                        "followers did not converge before the replication deadline");
        }
    }

    /**
     * Captures authority state and independently reads visible records and committed offset over
     * TCP.
     */
    public static ConsistencyObservation observe(
            String phase,
            ErrorCode error,
            ClusterHarness cluster,
            TopicPartition tp,
            OptionalLong position,
            String group,
            EffectRecorder effects,
            Optional<AppendResult> receipt) {
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(tp, "tp");
        Objects.requireNonNull(position, "position");
        Objects.requireNonNull(receipt, "receipt");

        ReplicationSnapshot replication = cluster.snapshot(tp);
        Map<Integer, Long> physicalLeo = new LinkedHashMap<>();
        for (int brokerId : ClusterHarness.BROKER_IDS)
            physicalLeo.put(brokerId, cluster.partitionLog(brokerId, tp).logEndOffset());

        var metadata = cluster.authority().metadata(tp);
        Endpoint endpoint = cluster.endpoint(metadata.leaderId());
        List<LogRecord> visible;
        OptionalLong committedOffset = OptionalLong.empty();
        try (RpcClient client = new RpcClient(endpoint, RPC_TIMEOUT_MILLIS)) {
            Messages.Reply fetchReply =
                    client.call(
                            new Messages.FetchRequest(
                                    tp,
                                    metadata.epoch(),
                                    0,
                                    OBSERVATION_MAX_RECORDS,
                                    MessageCodec.MAX_FETCH_BYTE_BUDGET,
                                    null));
            if (fetchReply.error() != ErrorCode.NONE
                    || !(fetchReply.body() instanceof Messages.FetchBody fetchBody))
                throw replyFailure("observation fetch", fetchReply);
            visible = List.copyOf(fetchBody.records());

            if (group != null) {
                Messages.Reply offsetReply =
                        client.call(new Messages.FetchOffsetRequest(new OffsetKey(group, tp)));
                if (offsetReply.error() != ErrorCode.NONE
                        || !(offsetReply.body() instanceof Messages.OffsetBody offsetBody))
                    throw replyFailure("observation offset fetch", offsetReply);
                committedOffset = offsetBody.nextOffset();
            }
        }

        return new ConsistencyObservation(
                phase,
                error,
                replication,
                physicalLeo,
                position,
                committedOffset,
                visible,
                effects == null ? List.of() : effects.snapshot(),
                receipt);
    }

    /** Waits on the authority partition condition until the current leader's LEO is reported. */
    public static void awaitLeaderLeo(ClusterHarness cluster, TopicPartition tp, long expectedLeo) {
        Objects.requireNonNull(cluster, "cluster");
        Objects.requireNonNull(tp, "tp");
        if (expectedLeo < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "expected leader LEO is negative");

        PartitionState state = cluster.authority().partitionState(tp);
        long deadline = System.nanoTime() + OPERATION_TIMEOUT_NANOS;
        state.lock.lock();
        try {
            int leaderId = state.leaderId;
            int epoch = state.epoch;
            while (state.reportedLEO.getOrDefault(leaderId, 0L) < expectedLeo) {
                if (state.leaderId != leaderId || state.epoch != epoch)
                    throw new CourseException(
                            ErrorCode.FENCED_EPOCH,
                            "leader changed while waiting for its reported LEO");
                long remaining = deadline - System.nanoTime();
                if (remaining <= 0)
                    throw new CourseException(
                            ErrorCode.REQUEST_TIMEOUT,
                            "leader LEO report did not arrive before the deadline");
                try {
                    state.changed.awaitNanos(remaining);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    throw new CourseException(
                            ErrorCode.REQUEST_TIMEOUT,
                            "interrupted while waiting for leader LEO",
                            exception);
                }
            }
        } finally {
            state.lock.unlock();
        }
    }

    private static void ensureSameLeader(
            ClusterHarness cluster, TopicPartition tp, int expectedLeader, int expectedEpoch) {
        var metadata = cluster.authority().metadata(tp);
        if (metadata.leaderId() != expectedLeader || metadata.epoch() != expectedEpoch)
            throw new CourseException(
                    ErrorCode.FENCED_EPOCH, "leader changed during follower replication");
    }

    private static CourseException replyFailure(String operation, Messages.Reply reply) {
        if (reply.error() != ErrorCode.NONE)
            return new CourseException(reply.error(), operation + " failed: " + reply.body());
        return new CourseException(
                ErrorCode.INVALID_REQUEST, operation + " returned an invalid body");
    }
}
