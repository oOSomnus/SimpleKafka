package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.lab.ConsistencyExperiments;
import io.simplekafka.lab.ConsistencyObservation;
import io.simplekafka.lab.ConsistencyTrace;
import io.simplekafka.lab.EffectRecorder;
import io.simplekafka.lab.FaultProxy;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.concurrent.atomic.AtomicLong;

class Step29Test {
    @TempDir Path root;

    @Test
    @DisplayName("Fault proxy rejects unknown API ids and a second fault arm")
    void faultProxyValidatesApiAndSingleFaultArming() {
        try (FaultProxy proxy = new FaultProxy(new Endpoint("127.0.0.1", 1))) {
            CourseException unknownApi =
                    assertThrows(CourseException.class, () -> proxy.dropNextReply((short) 0));
            assertEquals(ErrorCode.INVALID_REQUEST, unknownApi.code());
            proxy.dropNextReply(Api.PRODUCE);
            CourseException duplicateArm =
                    assertThrows(
                            CourseException.class,
                            () -> proxy.rejectNextRequest(Api.COMMIT_OFFSET));
            assertEquals(ErrorCode.INVALID_REQUEST, duplicateArm.code());
            assertTrue(proxy.armed());
        }
    }

    @Test
    @DisplayName("Lost producer reply is distinct from explicit resend")
    void producerOutcomeUnknownAndExplicitResendAreObserved() {
        TopicPartition tp = new TopicPartition("step29producer", 0);
        RecordData data = record("customer-A", "request-unknown-73", 1_723);
        try (ClusterHarness cluster = cluster(root.resolve("producer"), tp);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            assertNotNull(proxy.start());
            ConsistencyTrace trace =
                    ConsistencyExperiments.producerOutcomeUnknown(cluster, tp, data, proxy);
            assertEquals(tp, trace.tp());
            assertEquals(3, trace.observations().size());

            assertObservation(
                    trace.observations().get(0),
                    "UNKNOWN",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    List.of(1, 2, 3),
                    0,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            assertObservation(
                    trace.observations().get(1),
                    "POISONED",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    List.of(1, 2, 3),
                    0,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            List<LogRecord> duplicated = List.of(new LogRecord(0, data), new LogRecord(1, data));
            assertObservation(
                    trace.observations().get(2),
                    "EXPLICIT_RESEND",
                    ErrorCode.NONE,
                    1,
                    0,
                    List.of(1, 2, 3),
                    2,
                    Map.of(1, 2L, 2, 2L, 3, 2L),
                    Map.of(1, 2L, 2, 2L, 3, 2L),
                    OptionalLong.of(2),
                    OptionalLong.empty(),
                    duplicated,
                    List.of(),
                    Optional.of(new AppendResult(1, 2)));

            assertCluster(
                    cluster,
                    tp,
                    1,
                    0,
                    List.of(1, 2, 3),
                    2,
                    Map.of(1, 2L, 2, 2L, 3, 2L),
                    Map.of(1, duplicated, 2, duplicated, 3, duplicated));
            assertEquals(2, proxy.forwarded(Api.PRODUCE));
            assertEquals(1, proxy.dropped(Api.PRODUCE));
            assertEquals(0, proxy.rejected(Api.PRODUCE));
            assertFalse(proxy.armed());
        }
    }

    @Test
    @DisplayName("Leader acknowledgment can precede an elected log without the tail")
    void leaderAcknowledgmentDoesNotProtectAnUncommittedTail() {
        TopicPartition tp = new TopicPartition("step29leaderack", 0);
        RecordData data = record("leader-tail", "acked-but-uncommitted", 2_419);
        try (ClusterHarness cluster = cluster(root.resolve("leader-ack"), tp)) {
            ConsistencyTrace trace = ConsistencyExperiments.leaderAckLoss(cluster, tp, data);
            assertEquals(3, trace.observations().size());
            assertObservation(
                    trace.observations().get(0),
                    "ACKED_NOT_VISIBLE",
                    ErrorCode.NONE,
                    1,
                    0,
                    List.of(1, 2, 3),
                    0,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.of(new AppendResult(0, 1)));
            assertObservation(
                    trace.observations().get(1),
                    "ELECTED_WITHOUT_TAIL",
                    ErrorCode.NONE,
                    2,
                    1,
                    List.of(2),
                    0,
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            assertObservation(
                    trace.observations().get(2),
                    "OLD_TAIL_REMOVED",
                    ErrorCode.NONE,
                    2,
                    1,
                    List.of(1, 2),
                    0,
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            assertCluster(
                    cluster,
                    tp,
                    2,
                    1,
                    List.of(1, 2),
                    0,
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    Map.of(1, List.of(), 2, List.of(), 3, List.of()));
        }
    }

    @Test
    @DisplayName("ALL timeout can become visible after replication")
    void allAcknowledgmentTimeoutCanBecomeVisibleLater() {
        TopicPartition tp = new TopicPartition("step29alltimeout", 0);
        RecordData data = record("all-key", "late-visible-value", 3_101);
        List<LogRecord> visible = List.of(new LogRecord(0, data));
        try (ClusterHarness cluster = cluster(root.resolve("all-timeout"), tp)) {
            ConsistencyTrace trace = ConsistencyExperiments.allAckTimeout(cluster, tp, data);
            assertEquals(2, trace.observations().size());
            assertObservation(
                    trace.observations().get(0),
                    "TIMEOUT_NOT_VISIBLE",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    List.of(1, 2, 3),
                    0,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            assertObservation(
                    trace.observations().get(1),
                    "LATE_VISIBLE",
                    ErrorCode.NONE,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    OptionalLong.of(1),
                    OptionalLong.empty(),
                    visible,
                    List.of(),
                    Optional.empty());
            assertCluster(
                    cluster,
                    tp,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, visible, 2, visible, 3, visible));
        }
    }

    @Test
    @DisplayName("A failed consumer commit replays the side effect on a fresh consumer")
    void consumerCommitFailureReplaysEffectAndCommitsOnFreshConnection() throws Exception {
        TopicPartition tp = new TopicPartition("step29consumer", 0);
        String group = "step29-replay-group";
        RecordData data = record("order-87", "effect-after-poll", 4_003);
        LogRecord record = new LogRecord(0, data);
        EffectRecorder effects = new EffectRecorder();
        try (ClusterHarness cluster = cluster(root.resolve("consumer"), tp);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            proxy.start();
            ConsistencyTrace trace =
                    ConsistencyExperiments.consumerReplay(cluster, tp, group, data, effects, proxy);
            assertEquals(2, trace.observations().size());
            assertObservation(
                    trace.observations().get(0),
                    "COMMIT_FAILED",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    OptionalLong.of(1),
                    OptionalLong.empty(),
                    List.of(record),
                    List.of(record),
                    Optional.empty());
            assertObservation(
                    trace.observations().get(1),
                    "REPLAY_COMMITTED",
                    ErrorCode.NONE,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    OptionalLong.of(1),
                    OptionalLong.of(1),
                    List.of(record),
                    List.of(record, record),
                    Optional.empty());

            assertEquals(List.of(record, record), effects.snapshot());
            assertEquals(OptionalLong.of(1), committedOffset(cluster, group, tp));
            assertCluster(
                    cluster,
                    tp,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, List.of(record), 2, List.of(record), 3, List.of(record)));
            assertEquals(1, proxy.rejected(Api.COMMIT_OFFSET));
            assertEquals(0, proxy.forwarded(Api.COMMIT_OFFSET));
            assertFalse(proxy.armed());
        }
    }

    @Test
    @DisplayName("A stale group generation cannot commit or fetch but cannot undo its side effect")
    void staleMemberSideEffectSurvivesGenerationFencing() {
        TopicPartition tp = new TopicPartition("step29stale", 0);
        String group = "step29-stale-group";
        RecordData data = record("inventory-13", "old-owner-effect", 5_007);
        LogRecord record = new LogRecord(0, data);
        EffectRecorder effects = new EffectRecorder();
        try (ClusterHarness cluster = cluster(root.resolve("stale"), tp)) {
            ConsistencyTrace trace =
                    ConsistencyExperiments.staleGroupSideEffect(cluster, tp, group, data, effects);
            assertEquals(2, trace.observations().size());
            assertObservation(
                    trace.observations().get(0),
                    "STALE_COMMIT_REJECTED",
                    ErrorCode.ILLEGAL_GENERATION,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    OptionalLong.of(1),
                    OptionalLong.empty(),
                    List.of(record),
                    List.of(record),
                    Optional.empty());
            assertObservation(
                    trace.observations().get(1),
                    "NEW_OWNER_COMMITTED",
                    ErrorCode.NONE,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    OptionalLong.of(1),
                    OptionalLong.of(1),
                    List.of(record),
                    List.of(record, record),
                    Optional.empty());
            assertEquals(List.of(record, record), effects.snapshot());
            assertEquals(OptionalLong.of(1), committedOffset(cluster, group, tp));
            assertCluster(
                    cluster,
                    tp,
                    1,
                    0,
                    List.of(1, 2, 3),
                    1,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    Map.of(1, List.of(record), 2, List.of(record), 3, List.of(record)));
        }
    }

    @Test
    @DisplayName("Experiments reject stamped input before mutating the cluster")
    void rejectsStampedScenarioInputBeforeMutation() {
        TopicPartition tp = new TopicPartition("step29stamped", 0);
        RecordData stamped =
                new RecordData(
                        "producer-stamp".getBytes(StandardCharsets.UTF_8),
                        "not-an-ordinary-record".getBytes(StandardCharsets.UTF_8),
                        6_037,
                        new ProducerStamp(7, 0, 0, 1, 0, new byte[32]));
        try (ClusterHarness cluster = cluster(root.resolve("stamped"), tp)) {
            CourseException failure =
                    assertThrows(
                            CourseException.class,
                            () -> ConsistencyExperiments.leaderAckLoss(cluster, tp, stamped));
            assertEquals(ErrorCode.INVALID_REQUEST, failure.code());
            assertCluster(
                    cluster,
                    tp,
                    1,
                    0,
                    List.of(1, 2, 3),
                    0,
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    Map.of(1, List.of(), 2, List.of(), 3, List.of()));
        }
    }

    @Test
    @DisplayName("Experiments reject a nonempty starting log without adding another record")
    void rejectsInvalidInitialStateWithoutSideEffects() throws Exception {
        TopicPartition tp = new TopicPartition("step29invalid", 0);
        RecordData seed = record("seed-key", "already-present", 6_011);
        RecordData requested = record("new-key", "must-not-append", 6_019);
        EffectRecorder effects = new EffectRecorder();
        try (ClusterHarness cluster = cluster(root.resolve("invalid"), tp);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            proxy.start();
            appendSeed(cluster, tp, seed);
            CourseException outcomeUnknown =
                    assertThrows(
                            CourseException.class,
                            () ->
                                    ConsistencyExperiments.producerOutcomeUnknown(
                                            cluster, tp, requested, proxy));
            CourseException leaderAck =
                    assertThrows(
                            CourseException.class,
                            () -> ConsistencyExperiments.leaderAckLoss(cluster, tp, requested));
            CourseException allTimeout =
                    assertThrows(
                            CourseException.class,
                            () -> ConsistencyExperiments.allAckTimeout(cluster, tp, requested));
            CourseException replay =
                    assertThrows(
                            CourseException.class,
                            () ->
                                    ConsistencyExperiments.consumerReplay(
                                            cluster,
                                            tp,
                                            "invalid-group",
                                            requested,
                                            effects,
                                            proxy));
            CourseException stale =
                    assertThrows(
                            CourseException.class,
                            () ->
                                    ConsistencyExperiments.staleGroupSideEffect(
                                            cluster, tp, "invalid-group", requested, effects));
            assertEquals(ErrorCode.INVALID_REQUEST, outcomeUnknown.code());
            assertEquals(ErrorCode.INVALID_REQUEST, leaderAck.code());
            assertEquals(ErrorCode.INVALID_REQUEST, allTimeout.code());
            assertEquals(ErrorCode.INVALID_REQUEST, replay.code());
            assertEquals(ErrorCode.INVALID_REQUEST, stale.code());
            assertEquals(1, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(0, cluster.partitionLog(2, tp).logEndOffset());
            assertEquals(0, cluster.partitionLog(3, tp).logEndOffset());
            assertEquals(List.of(), effects.snapshot());
            assertFalse(proxy.armed());
            assertEquals(0, proxy.forwarded(Api.PRODUCE));
            assertEquals(0, proxy.rejected(Api.COMMIT_OFFSET));
        }
    }

    private static ClusterHarness cluster(Path directory, TopicPartition tp) {
        ClusterHarness cluster = new ClusterHarness(directory, new AtomicLong(0)::get);
        cluster.start();
        cluster.createTopic(tp.topic(), 1, 2);
        return cluster;
    }

    private static void appendSeed(ClusterHarness cluster, TopicPartition tp, RecordData data) {
        try (RpcClient client = cluster.client(1, 5_000)) {
            Messages.Reply reply =
                    client.call(
                            new Messages.ProduceRequest(
                                    tp, 0, io.simplekafka.model.Acks.LEADER, 5_000, List.of(data)));
            assertEquals(ErrorCode.NONE, reply.error());
        }
    }

    private static OptionalLong committedOffset(
            ClusterHarness cluster, String group, TopicPartition tp) {
        try (RpcClient client =
                cluster.client(cluster.authority().metadata(tp).leaderId(), 5_000)) {
            Messages.Reply reply =
                    client.call(new Messages.FetchOffsetRequest(new OffsetKey(group, tp)));
            assertEquals(ErrorCode.NONE, reply.error());
            return ((Messages.OffsetBody) reply.body()).nextOffset();
        }
    }

    private static void assertObservation(
            ConsistencyObservation observation,
            String phase,
            ErrorCode error,
            int leaderId,
            int epoch,
            List<Integer> isr,
            long highWatermark,
            Map<Integer, Long> perReplicaLeo,
            Map<Integer, Long> physicalLeo,
            OptionalLong position,
            OptionalLong committedOffset,
            List<LogRecord> visible,
            List<LogRecord> effects,
            Optional<AppendResult> receipt) {
        assertEquals(phase, observation.phase());
        assertEquals(error, observation.error());
        assertSnapshot(
                observation.replication(), leaderId, epoch, isr, highWatermark, perReplicaLeo);
        assertEquals(physicalLeo, observation.physicalLeo());
        assertEquals(position, observation.position());
        assertEquals(committedOffset, observation.committedOffset());
        assertEquals(visible, observation.visible());
        assertEquals(effects, observation.effects());
        assertEquals(receipt, observation.receipt());
    }

    private static void assertCluster(
            ClusterHarness cluster,
            TopicPartition tp,
            int leaderId,
            int epoch,
            List<Integer> isr,
            long highWatermark,
            Map<Integer, Long> physicalLeo,
            Map<Integer, List<LogRecord>> records) {
        ReplicationSnapshot snapshot = cluster.snapshot(tp);
        assertSnapshot(snapshot, leaderId, epoch, isr, highWatermark, physicalLeo);
        for (int brokerId : ClusterHarness.BROKER_IDS) {
            assertEquals(
                    physicalLeo.get(brokerId), cluster.partitionLog(brokerId, tp).logEndOffset());
            assertEquals(
                    records.get(brokerId), cluster.partitionLog(brokerId, tp).read(0, 10, 65_536));
        }
    }

    private static void assertSnapshot(
            ReplicationSnapshot snapshot,
            int leaderId,
            int epoch,
            List<Integer> isr,
            long highWatermark,
            Map<Integer, Long> perReplicaLeo) {
        assertEquals(leaderId, snapshot.leaderId());
        assertEquals(epoch, snapshot.epoch());
        assertEquals(List.of(1, 2, 3), snapshot.replicas());
        assertEquals(isr, snapshot.isr());
        assertEquals(highWatermark, snapshot.highWatermark());
        assertEquals(perReplicaLeo, snapshot.perReplicaLEO());
    }

    private static RecordData record(String key, String value, long timestamp) {
        return new RecordData(
                key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8),
                timestamp);
    }
}
