package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.client.GroupConsumer;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.replication.ReplicaAdmission;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step28Test {
    @TempDir Path root;

    @Test
    void recoveredProofRestoresIsrForAllAcksAndReplicaLogsConverge() throws Exception {
        TopicPartition tp = new TopicPartition("step28", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<Endpoint> bootstrap = List.of(cluster.endpoint(1), cluster.endpoint(2), cluster.endpoint(3));
            RpcClientFactory clients = endpoint -> new RpcClient(endpoint, 2_000);
            try (MetadataRouter router = new MetadataRouter(bootstrap, clients);
                 GroupConsumer groupConsumer = new GroupConsumer(router, "step28-workers", "a")) {
                List<RecordData> prefix = List.of(record("zero", 20), record("one", 21));
                assertEquals(new AppendResult(0, 2), produce(cluster, tp, 1, 0, Acks.LEADER, prefix).result());
                for (int brokerId : List.of(2, 3)) {
                    assertEquals(2, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                    cluster.tracker(tp).report(brokerId, 0, 2);
                }
                assertEquals(2, cluster.tracker(tp).highWatermark());
                groupConsumer.subscribe(tp.topic());
                assertEquals(prefix, groupConsumer.poll(10, 65_536).get(tp).stream().map(LogRecord::data).toList());
                groupConsumer.commitSync();
                OffsetKey committedKey = new OffsetKey("step28-workers", tp);
                assertEquals(2, committedOffset(router, committedKey));

                Messages.GroupBody secondMember;
                try (RpcClient client = new RpcClient(cluster.endpoint(2), 2_000)) {
                    Messages.Reply joined = client.call(new Messages.JoinGroupRequest("step28-workers", "b", tp.topic()));
                    assertEquals(ErrorCode.NONE, joined.error());
                    secondMember = (Messages.GroupBody) joined.body();
                }
                assertEquals(2, secondMember.assignment().generation());
                CourseException staleCommit = org.junit.jupiter.api.Assertions.assertThrows(CourseException.class,
                        groupConsumer::commitSync);
                assertEquals(ErrorCode.ILLEGAL_GENERATION, staleCommit.code());
                assertEquals(2, committedOffset(router, committedKey),
                        "a stale generation must not move the committed offset");
                groupConsumer.subscribe(tp.topic());

                cluster.stopBroker(1);
                assertEquals(2, cluster.elect(tp).leaderId());
                assertEquals(1, cluster.snapshot(tp).epoch());
                router.refresh(tp.topic());
                assertEquals(2, cluster.reconcile(3, tp));
                assertTrue(cluster.admit(3, tp));
                ReplicationSnapshot afterAdmission = cluster.snapshot(tp);
                assertTrue(cluster.admit(3, tp));
                assertEquals(afterAdmission, cluster.snapshot(tp));
                assertEquals(List.of(2, 3), afterAdmission.isr());

                AppendResult firstAfterElection = produceAllWhileReplicating(cluster, tp, 3,
                        record("after-election", 22), List.of(3));
                assertEquals(new AppendResult(2, 3), firstAfterElection);
                assertEquals(3, cluster.tracker(tp).highWatermark());
                assertEquals(List.of(record("after-election", 22)),
                        groupConsumer.poll(10, 65_536).get(tp).stream().map(LogRecord::data).toList());
                groupConsumer.commitSync();
                assertEquals(3, committedOffset(router, committedKey));

                cluster.restartBroker(1);
                assertEquals(3, cluster.reconcile(1, tp));
                assertTrue(cluster.admit(1, tp));
                assertEquals(List.of(1, 2, 3), cluster.snapshot(tp).isr());

                AppendResult secondAfterElection = produceAllWhileReplicating(cluster, tp, 4,
                        record("after-rejoin", 23), List.of(1, 3));
                assertEquals(new AppendResult(3, 4), secondAfterElection);
                assertEquals(4, cluster.tracker(tp).highWatermark());
                assertEquals(List.of(record("after-rejoin", 23)),
                        groupConsumer.poll(10, 65_536).get(tp).stream().map(LogRecord::data).toList());
                groupConsumer.commitSync();
                assertEquals(4, committedOffset(router, committedKey));

                List<LogRecord> leaderRecords = cluster.partitionLog(2, tp).read(0, 10, 65_536);
                assertEquals(4, leaderRecords.size());
                List<Long> expectedOffsets = List.of(0L, 1L, 2L, 3L);
                assertEquals(expectedOffsets, leaderRecords.stream().map(LogRecord::offset).toList());
                for (int brokerId : List.of(1, 3)) {
                    List<LogRecord> replicaRecords = cluster.partitionLog(brokerId, tp).read(0, 10, 65_536);
                    assertEquals(leaderRecords, replicaRecords, "replicas must converge by logical offset and record data");
                    assertEquals(expectedOffsets, replicaRecords.stream().map(LogRecord::offset).toList());
                }
            }
        }
    }

    @Test
    void proofBecomesStaleWhenLeaderAppendsAfterReconciliation() {
        TopicPartition tp = new TopicPartition("step28proof", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = List.of(record("committed", 30));
            assertEquals(new AppendResult(0, 1), produce(cluster, tp, 1, 0, Acks.LEADER, committed).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 1);
            }
            assertEquals(1, cluster.tracker(tp).highWatermark());

            cluster.stopBroker(1);
            assertEquals(2, cluster.elect(tp).leaderId());
            cluster.restartBroker(1);
            assertEquals(1, cluster.reconcile(1, tp));

            assertEquals(new AppendResult(1, 2), produce(cluster, tp, 2, 1, Acks.LEADER,
                    List.of(record("leader-advanced", 31))).result());
            ReplicationSnapshot beforeStaleAdmission = cluster.snapshot(tp);
            assertFalse(admission(cluster, 1, tp).tryAdd(1, tp, beforeStaleAdmission.epoch()),
                    "a proof for the previous leader LEO must not admit a stale copy");
            assertEquals(beforeStaleAdmission, cluster.snapshot(tp));
            assertEquals(List.of(2), beforeStaleAdmission.isr());
            assertEquals(1, cluster.partitionLog(1, tp).logEndOffset(),
                    "a stale proof attempt must not copy or otherwise mutate the local log");

            assertEquals(2, cluster.reconcile(1, tp));
            assertTrue(admission(cluster, 1, tp).tryAdd(1, tp, cluster.snapshot(tp).epoch()));
            assertEquals(2, cluster.snapshot(tp).highWatermark());
            assertEquals(List.of(1, 2), cluster.snapshot(tp).isr());
        }
    }

    @Test
    void admissionRequiresCurrentProofAndOnlineAssignedFollowerAndIsIdempotent() {
        TopicPartition tp = new TopicPartition("step28admission", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = List.of(
                    record("prefix-0", 40), record("prefix-1", 41), record("prefix-2", 42));
            assertEquals(new AppendResult(0, 3), produce(cluster, tp, 1, 0, Acks.LEADER, committed).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(3, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 3);
            }
            assertEquals(3, cluster.tracker(tp).highWatermark());

            cluster.stopBroker(1);
            assertEquals(2, cluster.elect(tp).leaderId());
            cluster.restartBroker(1);
            ReplicationSnapshot beforeUnprovedAttempt = cluster.snapshot(tp);
            assertFalse(admission(cluster, 1, tp).tryAdd(1, tp, beforeUnprovedAttempt.epoch()),
                    "a matching local LEO is not evidence of reconciliation");
            assertFalse(admission(cluster, 2, tp).tryAdd(2, tp, beforeUnprovedAttempt.epoch()),
                    "the current leader cannot be admitted as its own follower");
            assertEquals(beforeUnprovedAttempt, cluster.snapshot(tp));
            assertEquals(3, cluster.partitionLog(1, tp).logEndOffset());

            assertEquals(3, cluster.reconcile(1, tp));
            ReplicationSnapshot beforeStaleEpoch = cluster.snapshot(tp);
            assertFalse(admission(cluster, 1, tp).tryAdd(1, tp, beforeStaleEpoch.epoch() - 1),
                    "proofs from an earlier epoch cannot admit a replica");
            assertEquals(beforeStaleEpoch, cluster.snapshot(tp));
            assertEquals(3, cluster.partitionLog(1, tp).logEndOffset());

            assertTrue(admission(cluster, 1, tp).tryAdd(1, tp, beforeStaleEpoch.epoch()));
            ReplicationSnapshot afterAdmission = cluster.snapshot(tp);
            assertEquals(List.of(1, 2), afterAdmission.isr());
            assertEquals(3, afterAdmission.highWatermark());
            assertTrue(admission(cluster, 1, tp).tryAdd(1, tp, afterAdmission.epoch()),
                    "re-admitting an unchanged ISR member is idempotently successful");
            assertEquals(afterAdmission, cluster.snapshot(tp));

            CourseException unassigned = assertThrows(CourseException.class,
                    () -> admission(cluster, 1, tp).tryAdd(4, tp, afterAdmission.epoch()));
            assertEquals(ErrorCode.INVALID_REQUEST, unassigned.code());
            assertEquals(afterAdmission, cluster.snapshot(tp));
            assertEquals(3, cluster.partitionLog(1, tp).logEndOffset());

            cluster.stopBroker(1);
            assertFalse(admission(cluster, 1, tp).tryAdd(1, tp, afterAdmission.epoch()),
                    "offline replicas cannot be admitted even when they remain in the ISR");
            assertEquals(afterAdmission, cluster.snapshot(tp));
            assertEquals(3, cluster.partitionLog(1, tp).logEndOffset());
        }
    }

    @Test
    void recoveryProofCannotBeUsedWithAnotherLogObjectWithTheSameRecordsAndVersion() {
        TopicPartition tp = new TopicPartition("step28-log-identity", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = List.of(
                    record("identity-0", 80), record("identity-1", 81), record("identity-2", 82));
            assertEquals(new AppendResult(0, 3), produce(cluster, tp, 1, 0, Acks.LEADER, committed).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(3, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 3);
            }
            assertEquals(3, cluster.tracker(tp).highWatermark());

            cluster.stopBroker(1);
            assertEquals(2, cluster.elect(tp).leaderId());
            cluster.restartBroker(1);
            assertEquals(3, cluster.reconcile(1, tp));
            PartitionLog recoveredLog = cluster.partitionLog(1, tp);
            ReplicationSnapshot beforeWrongLog = cluster.snapshot(tp);

            try (PartitionLog differentLog = new PartitionLog(root.resolve("different-log"), 256, 1)) {
                differentLog.append(committed);
                assertEquals(recoveredLog.logEndOffset(), differentLog.logEndOffset());
                assertEquals(recoveredLog.mutationVersion(), differentLog.mutationVersion());
                assertEquals(recoveredLog.read(0, 10, 65_536), differentLog.read(0, 10, 65_536));

                ReplicaAdmission admission = new ReplicaAdmission(cluster.authority(), differentLog);
                assertFalse(admission.tryAdd(1, tp, beforeWrongLog.epoch()),
                        "matching content, LEO and generation do not replace the proved log identity");
                assertEquals(beforeWrongLog, cluster.snapshot(tp));
                assertEquals(3, recoveredLog.logEndOffset());
            }

            assertEquals(3, cluster.reconcile(1, tp), "the failed admission consumed the prior proof");
            assertTrue(admission(cluster, 1, tp).tryAdd(1, tp, cluster.snapshot(tp).epoch()));
            assertEquals(List.of(1, 2), cluster.snapshot(tp).isr());
        }
    }

    @Test
    void admissionRejectsProofAfterTruncateAndAfterDifferentDataRestoresSameLeo() {
        TopicPartition truncated = new TopicPartition("step28-truncated", 0);
        TopicPartition rewritten = new TopicPartition("step28-rewritten", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            List<RecordData> committed = List.of(
                    record("prefix-0", 50), record("prefix-1", 51), record("prefix-2", 52));
            for (TopicPartition tp : List.of(truncated, rewritten)) {
                cluster.createTopic(tp.topic(), 1, 2);
                assertEquals(new AppendResult(0, 3), produce(cluster, tp, 1, 0, Acks.LEADER, committed).result());
                for (int brokerId : List.of(2, 3)) {
                    assertEquals(3, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                    cluster.tracker(tp).report(brokerId, 0, 3);
                }
                assertEquals(3, cluster.tracker(tp).highWatermark());
            }

            cluster.stopBroker(1);
            for (TopicPartition tp : List.of(truncated, rewritten))
                assertEquals(2, cluster.elect(tp).leaderId());
            cluster.restartBroker(1);
            for (TopicPartition tp : List.of(truncated, rewritten))
                assertEquals(3, cluster.reconcile(1, tp));

            ReplicationSnapshot beforeTruncateAttempt = cluster.snapshot(truncated);
            cluster.partitionLog(1, truncated).truncateTo(0);
            assertEquals(0, cluster.partitionLog(1, truncated).logEndOffset());
            assertFalse(admission(cluster, 1, truncated).tryAdd(1, truncated, beforeTruncateAttempt.epoch()),
                    "a proof cannot survive a local truncation that lowers LEO");
            assertEquals(beforeTruncateAttempt, cluster.snapshot(truncated));
            assertEquals(0, cluster.partitionLog(1, truncated).logEndOffset());

            ReplicationSnapshot beforeRewriteAttempt = cluster.snapshot(rewritten);
            cluster.partitionLog(1, rewritten).truncateTo(2);
            assertEquals(new AppendResult(2, 3),
                    cluster.partitionLog(1, rewritten).append(List.of(record("different-at-2", 53))));
            List<LogRecord> divergentReplica = cluster.partitionLog(1, rewritten).read(0, 10, 65_536);
            assertEquals(List.of(committed.get(0), committed.get(1), record("different-at-2", 53)),
                    divergentReplica.stream().map(LogRecord::data).toList());
            assertEquals(List.of(0L, 1L, 2L), divergentReplica.stream().map(LogRecord::offset).toList());
            assertFalse(admission(cluster, 1, rewritten).tryAdd(1, rewritten, beforeRewriteAttempt.epoch()),
                    "same LEO does not make a proof fresh after truncate and replacement append");
            assertEquals(beforeRewriteAttempt, cluster.snapshot(rewritten));
            assertEquals(3, cluster.partitionLog(1, rewritten).logEndOffset());

            CourseException conflict = assertThrows(CourseException.class, () -> cluster.reconcile(1, rewritten));
            assertEquals(ErrorCode.CORRUPT_RECORD, conflict.code());
            assertEquals(List.of(committed.get(0), committed.get(1), record("different-at-2", 53)),
                    cluster.partitionLog(1, rewritten).read(0, 10, 65_536).stream()
                            .map(LogRecord::data).toList());
            assertEquals(beforeRewriteAttempt, cluster.snapshot(rewritten));
        }
    }

    @Test
    void minIsrThreeNeedsBothRecoveredReplicasBeforeAllAcksResume() throws Exception {
        TopicPartition tp = new TopicPartition("step28-min-isr-three", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 3);
            List<RecordData> committed = List.of(
                    record("prefix-0", 60), record("prefix-1", 61), record("prefix-2", 62));
            assertEquals(new AppendResult(0, 3), produce(cluster, tp, 1, 0, Acks.LEADER, committed).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(3, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 3);
            }
            assertEquals(3, cluster.tracker(tp).highWatermark());

            cluster.stopBroker(1);
            cluster.stopBroker(3);
            assertEquals(2, cluster.elect(tp).leaderId());
            assertEquals(List.of(2), cluster.snapshot(tp).isr());
            cluster.restartBroker(1);
            cluster.restartBroker(3);
            assertEquals(3, cluster.reconcile(1, tp));
            assertEquals(3, cluster.reconcile(3, tp));

            assertTrue(admission(cluster, 1, tp).tryAdd(1, tp, 1));
            ReplicationSnapshot twoReplicas = cluster.snapshot(tp);
            assertEquals(List.of(1, 2), twoReplicas.isr());
            assertEquals(3, twoReplicas.highWatermark());
            Messages.Reply rejectedAll;
            try (RpcClient client = new RpcClient(cluster.endpoint(2), 3_000)) {
                rejectedAll = client.call(new Messages.ProduceRequest(
                        tp, 1, Acks.ALL, 1_000, List.of(record("must-wait-for-third", 63))));
            }
            assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, rejectedAll.error());
            assertEquals(3, cluster.partitionLog(2, tp).logEndOffset(),
                    "ALL is rejected before append while minISR is unmet");
            assertEquals(twoReplicas, cluster.snapshot(tp));
            assertEquals(committed, cluster.partitionLog(2, tp).read(0, 10, 65_536)
                    .stream().map(LogRecord::data).toList());

            assertTrue(admission(cluster, 3, tp).tryAdd(3, tp, 1));
            assertEquals(List.of(1, 2, 3), cluster.snapshot(tp).isr());
            assertEquals(new AppendResult(3, 4), produceAllWhileReplicating(
                    cluster, tp, 4, record("all-restored", 64), List.of(1, 3)));
            assertEquals(4, cluster.tracker(tp).highWatermark());
            assertEquals(List.of(record("all-restored", 64)),
                    cluster.replicatedPartition(2, tp).fetch(3, 10, 65_536, 1)
                            .stream().map(LogRecord::data).toList());
        }
    }

    @Test
    void minIsrTwoAdmissionAdvancesHwAndWakesAnActualAllProduceWaiter() throws Exception {
        TopicPartition tp = new TopicPartition("step28-min-isr-two", 0);
        AtomicLong clock = new AtomicLong(0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = List.of(
                    record("prefix-0", 70), record("prefix-1", 71), record("prefix-2", 72));
            assertEquals(new AppendResult(0, 3), produce(cluster, tp, 1, 0, Acks.LEADER, committed).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(3, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 3);
            }
            assertEquals(3, cluster.tracker(tp).highWatermark());

            cluster.stopBroker(1);
            assertEquals(2, cluster.elect(tp).leaderId());
            assertEquals(3, cluster.reconcile(3, tp));
            assertTrue(admission(cluster, 3, tp).tryAdd(3, tp, 1));
            assertEquals(List.of(2, 3), cluster.snapshot(tp).isr());

            cluster.restartBroker(1);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Messages.Reply> reply = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                started.countDown();
                try (RpcClient client = new RpcClient(cluster.endpoint(2), 7_000)) {
                    reply.set(client.call(new Messages.ProduceRequest(
                            tp, 1, Acks.ALL, 5_000, List.of(record("all-pending", 73)))));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    completed.countDown();
                }
            }, "step28-admission-all-ack");
            producer.setDaemon(true);
            producer.start();
            try {
                assertTrue(started.await(2, TimeUnit.SECONDS));
                awaitLeaderEnd(cluster, tp, 4, producer);
                assertEquals(3, cluster.tracker(tp).highWatermark());
                assertTrue(cluster.replicatedPartition(2, tp).fetch(3, 10, 65_536, 1).isEmpty(),
                        "the appended record remains invisible below the high watermark");
                awaitConditionWaiter(cluster.authority().partitionState(tp), producer);
                assertFalse(completed.await(0, TimeUnit.NANOSECONDS));

                assertEquals(4, cluster.reconcile(1, tp));
                assertEquals(3, cluster.tracker(tp).highWatermark());
                assertTrue(cluster.replicatedPartition(2, tp).fetch(3, 10, 65_536, 1).isEmpty(),
                        "reconciliation alone does not commit or expose the tail record");
                clock.set(1_000);
                PartitionState state = cluster.authority().partitionState(tp);
                state.lock.lock();
                try {
                    assertEquals(Set.of(3), cluster.tracker(tp).expireLagging());
                    assertEquals(List.of(2), cluster.snapshot(tp).isr());
                    assertEquals(3, cluster.tracker(tp).highWatermark());
                    assertTrue(admission(cluster, 1, tp).tryAdd(1, tp, 1));
                    assertEquals(List.of(1, 2), cluster.snapshot(tp).isr());
                    assertEquals(4, cluster.tracker(tp).highWatermark());
                } finally {
                    state.lock.unlock();
                }

                assertTrue(completed.await(3, TimeUnit.SECONDS));
                producer.join(2_000);
                assertFalse(producer.isAlive());
                assertNull(failure.get());
                assertNotNull(reply.get());
                assertEquals(ErrorCode.NONE, reply.get().error());
                assertEquals(new AppendResult(3, 4), ((Messages.ProduceBody) reply.get().body()).result());
                List<LogRecord> leaderRecords = cluster.partitionLog(2, tp).read(0, 10, 65_536);
                assertEquals(List.of(0L, 1L, 2L, 3L),
                        leaderRecords.stream().map(LogRecord::offset).toList());
                assertEquals(leaderRecords, cluster.partitionLog(1, tp).read(0, 10, 65_536));
                assertEquals(List.of(record("all-pending", 73)),
                        cluster.replicatedPartition(2, tp).fetch(3, 10, 65_536, 1)
                                .stream().map(LogRecord::data).toList());
            } finally {
                if (producer.isAlive()) {
                    producer.interrupt();
                    producer.join(6_000);
                }
                assertFalse(producer.isAlive(), "the ALL producer thread must terminate");
            }
        }
    }

    private static void awaitConditionWaiter(PartitionState state, Thread producer) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        boolean waiting = false;
        while (!waiting && producer.isAlive() && System.nanoTime() < deadline) {
            state.lock.lock();
            try {
                waiting = state.lock.hasWaiters(state.changed);
            } finally {
                state.lock.unlock();
            }
            if (!waiting) Thread.onSpinWait();
        }
        assertTrue(waiting, "the actual ALL producer must be waiting for high-watermark progress");
    }

    private static ReplicaAdmission admission(ClusterHarness cluster, int brokerId, TopicPartition tp) {
        return new ReplicaAdmission(cluster.authority(), cluster.partitionLog(brokerId, tp));
    }

    private static AppendResult produceAllWhileReplicating(ClusterHarness cluster, TopicPartition tp,
                                                            long expectedLeo, RecordData record,
                                                            List<Integer> followers) throws Exception {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Messages.ProduceBody> reply = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread producer = new Thread(() -> {
            started.countDown();
            try {
                reply.set(produce(cluster, tp, 2, 1, Acks.ALL, List.of(record)));
            } catch (Throwable thrown) {
                failure.set(thrown);
            } finally {
                completed.countDown();
            }
        }, "step28-all-ack");
        producer.setDaemon(true);
        producer.start();
        assertTrue(started.await(2, TimeUnit.SECONDS));
        awaitLeaderEnd(cluster, tp, expectedLeo, producer);
        assertFalse(completed.await(0, TimeUnit.NANOSECONDS));

        for (int brokerId : followers) {
            long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
            int copied = 0;
            while (copied == 0 && producer.isAlive() && System.nanoTime() < deadline) {
                copied = cluster.replicator(brokerId, tp).pollOnce(10, 65_536);
                if (copied == 0) Thread.yield();
            }
            assertEquals(1, copied, "follower did not pull the pending record");
            cluster.tracker(tp).report(brokerId, 1, cluster.partitionLog(brokerId, tp).logEndOffset());
        }
        assertEquals(expectedLeo, cluster.tracker(tp).highWatermark());
        assertTrue(completed.await(3, TimeUnit.SECONDS));
        producer.join(2_000);
        assertFalse(producer.isAlive());
        assertNull(failure.get());
        return reply.get().result();
    }

    private static void awaitLeaderEnd(ClusterHarness cluster, TopicPartition tp, long expected, Thread producer) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (producer.isAlive() && cluster.partitionLog(2, tp).logEndOffset() < expected
                && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(expected, cluster.partitionLog(2, tp).logEndOffset(), "leader did not append before the deadline");
    }

    private static Messages.ProduceBody produce(ClusterHarness cluster, TopicPartition tp, int brokerId, int epoch,
                                                 Acks acks, List<RecordData> records) {
        try (RpcClient client = new RpcClient(cluster.endpoint(brokerId), 3_000)) {
            Messages.Reply reply = client.call(new Messages.ProduceRequest(tp, epoch, acks, 5_000, records));
            assertEquals(ErrorCode.NONE, reply.error());
            return (Messages.ProduceBody) reply.body();
        }
    }
    private static long committedOffset(MetadataRouter router, OffsetKey key) {
        Messages.Reply reply = router.controlCall(new Messages.FetchOffsetRequest(key));
        assertEquals(ErrorCode.NONE, reply.error());
        var offset = ((Messages.OffsetBody) reply.body()).nextOffset();
        assertTrue(offset.isPresent());
        return offset.getAsLong();
    }



    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
