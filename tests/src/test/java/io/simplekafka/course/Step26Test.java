package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.RecoveryProof;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.replication.ReplicaReconciler;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step26Test {
    @TempDir Path root;

    @Test
    void repairsOnlyDivergentUncommittedTailAtHighWatermarkAndReturnsLeaderPrefixProof() {
        TopicPartition tp = new TopicPartition("step26", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = prefix();
            assertEquals(3, checkpoint(cluster, tp, committed));
            assertEquals(new AppendResult(3, 4), ((Messages.ProduceBody) produce(cluster, tp, 1, 0,
                    Acks.LEADER, List.of(record("old-tail", 30))).body()).result());

            cluster.stopBroker(1);
            var elected = cluster.elect(tp);
            assertEquals(2, elected.leaderId());
            cluster.restartBroker(1);
            assertEquals(new AppendResult(3, 4), ((Messages.ProduceBody) produce(cluster, tp, 2, 1,
                    Acks.LEADER, List.of(record("new-tail", 31))).body()).result());
            List<LogRecord> leaderPrefix = cluster.partitionLog(2, tp).read(0, 10, 65_536);
            ReplicationSnapshot beforeRepair = cluster.snapshot(tp);
            assertEquals(3, beforeRepair.highWatermark());
            assertEquals(new LogRecord(3, record("new-tail", 31)), leaderPrefix.get(3),
                    "the first local/leader mismatch is exactly at the committed high-watermark");
            assertEquals(new LogRecord(3, record("old-tail", 30)),
                    cluster.partitionLog(1, tp).read(3, 1, 65_536).get(0));

            try (RpcClient client = new RpcClient(cluster.endpoint(2), 3_000)) {
                ReplicaReconciler reconciler = new ReplicaReconciler(1, tp, cluster.partitionLog(1, tp),
                        client, cluster.authority());
                assertEquals(4, reconciler.reconcile(1));
                assertEquals(leaderPrefix, cluster.partitionLog(1, tp).read(0, 10, 65_536));
                assertEquals(beforeRepair, cluster.snapshot(tp),
                        "recovery reads must not report progress or change ISR/HW");
                RecoveryProof proof = reconciler.recoveryProof().orElseThrow();
                assertEquals(1, proof.brokerId());
                assertEquals(tp, proof.tp());
                assertEquals(1, proof.epoch());
                assertEquals(4, proof.leaderLEO());
                assertEquals(4, proof.localLEO());
                assertEquals(3, proof.highWatermark());
            }
        }
    }

    @Test
    void refusesMismatchAtHighWatermarkMinusOneWithoutChangingTheLogOrAuthorityAndClearsOldProof()
            throws IOException {
        TopicPartition tp = new TopicPartition("step26corrupt", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(3, checkpoint(cluster, tp, prefix()));
            PartitionLog local = cluster.partitionLog(2, tp);
            try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
                ReplicaReconciler reconciler = new ReplicaReconciler(2, tp, local, client, cluster.authority());
                assertEquals(3, reconciler.reconcile(0));
                assertTrue(reconciler.recoveryProof().isPresent(), "successful repair publishes a proof");

                local.truncateTo(2);
                RecordData conflict = record("conflicting-committed-two", 99);
                assertEquals(new AppendResult(2, 3), local.append(List.of(conflict)));
                List<LogRecord> damagedBefore = local.read(0, 10, 65_536);
                assertEquals(new LogRecord(2, conflict), damagedBefore.get(2));
                ReplicationSnapshot before = cluster.snapshot(tp);
                Map<String, String> diskBefore = diskImage(logDirectory(cluster, 2, tp));

                CourseException corruption = assertThrows(CourseException.class, () -> reconciler.reconcile(0));
                assertEquals(ErrorCode.CORRUPT_RECORD, corruption.code());
                assertEquals(3, local.logEndOffset());
                assertEquals(damagedBefore, local.read(0, 10, 65_536));
                assertEquals(diskBefore, diskImage(logDirectory(cluster, 2, tp)));
                assertEquals(before, cluster.snapshot(tp));
                assertTrue(reconciler.recoveryProof().isEmpty(),
                        "a failed later repair must invalidate the earlier proof");
            }
        }
    }

    @Test
    void fillsShortAndEmptyReplicasWithTheCommittedPrefix() {
        TopicPartition tp = new TopicPartition("step26lagging", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = prefix();
            assertEquals(3, checkpoint(cluster, tp, committed));
            cluster.stopBroker(1);
            cluster.elect(tp);
            cluster.restartBroker(1);

            List<LogRecord> expected = expectedRecords(committed);
            PartitionLog local = cluster.partitionLog(1, tp);
            for (long shortLeo : List.of(1L, 0L)) {
                local.truncateTo(shortLeo);
                ReplicationSnapshot before = cluster.snapshot(tp);
                assertEquals(shortLeo, local.logEndOffset());
                assertEquals(3, cluster.reconcile(1, tp));
                assertEquals(expected, local.read(0, 10, 65_536));
                assertEquals(before, cluster.snapshot(tp),
                        "filling a replica must not report its progress or change ISR/HW");
            }
        }
    }

    @Test
    void trimsOnlyAnUncommittedTailWhenTheLocalReplicaHasTheCompleteLeaderPrefix() {
        TopicPartition tp = new TopicPartition("step26longer", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = prefix();
            assertEquals(3, checkpoint(cluster, tp, committed));

            PartitionLog local = cluster.partitionLog(2, tp);
            RecordData extra = record("local-only-tail", 40);
            assertEquals(new AppendResult(3, 4), local.append(List.of(extra)));
            List<LogRecord> beforeRepair = local.read(0, 10, 65_536);
            assertEquals(expectedRecords(committed).size() + 1, beforeRepair.size());
            assertEquals(new LogRecord(3, extra), beforeRepair.get(3));
            ReplicationSnapshot before = cluster.snapshot(tp);

            assertEquals(3, cluster.reconcile(2, tp));
            assertEquals(expectedRecords(committed), local.read(0, 10, 65_536));
            assertEquals(3, local.logEndOffset());
            assertEquals(before, cluster.snapshot(tp));
        }
    }

    @Test
    void repairsTenRecordsInFourRecordFetchRoundsWithoutReportingRecoveryProgress() {
        TopicPartition tp = new TopicPartition("step26rounds", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = records(10);
            assertEquals(10, checkpoint(cluster, tp, committed));

            PartitionLog leader = cluster.partitionLog(1, tp);
            PartitionLog local = cluster.partitionLog(2, tp);
            local.truncateTo(0);
            ReplicationSnapshot before = cluster.snapshot(tp);
            ConcurrentLinkedQueue<Long> requestedOffsets = new ConcurrentLinkedQueue<>();
            ConcurrentLinkedQueue<Integer> requestedRecordLimits = new ConcurrentLinkedQueue<>();
            try (BrokerServer scriptedLeader = new BrokerServer("127.0.0.1", 0, request -> {
                if (!(request instanceof Messages.ReplicaFetchRequest fetch))
                    return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "expected replica fetch");
                requestedOffsets.add(fetch.fetchOffset());
                requestedRecordLimits.add(fetch.maxRecords());
                return Messages.Reply.success(new Messages.ReplicaFetchBody(
                        leader.read(fetch.fetchOffset(), fetch.maxRecords(), fetch.maxBytes()),
                        fetch.epoch(), before.highWatermark(), leader.logEndOffset()));
            })) {
                Endpoint endpoint = scriptedLeader.start();
                try (RpcClient client = new RpcClient(endpoint, 3_000)) {
                    ReplicaReconciler reconciler = new ReplicaReconciler(
                            2, tp, local, client, cluster.authority());
                    assertEquals(10, reconciler.reconcile(0));
                    assertEquals(List.of(0L, 4L, 8L), List.copyOf(requestedOffsets));
                    assertEquals(List.of(4, 4, 4), List.copyOf(requestedRecordLimits));
                    assertEquals(expectedRecords(committed), leader.read(0, 20, 65_536));
                    assertEquals(expectedRecords(committed), local.read(0, 20, 65_536));
                    assertEquals(before, cluster.snapshot(tp),
                            "recovery fetches must not report progress or change ISR/HW");
                    assertEquals(10, reconciler.recoveryProof().orElseThrow().leaderLEO());
                }
            }
        }
    }

    @Test
    void rejectsStaleEpochLeaderSelfAndNonassignedEntryBeforeAnyRpcOrDiskChange() throws IOException {
        TopicPartition tp = new TopicPartition("step26entry", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(3, checkpoint(cluster, tp, prefix()));
            AtomicInteger rpcRequests = new AtomicInteger();
            PartitionLog leader = cluster.partitionLog(1, tp);
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            try (BrokerServer scriptedLeader = new BrokerServer("127.0.0.1", 0, request -> {
                rpcRequests.incrementAndGet();
                Messages.ReplicaFetchRequest fetch = (Messages.ReplicaFetchRequest) request;
                return Messages.Reply.success(new Messages.ReplicaFetchBody(
                        leader.read(fetch.fetchOffset(), fetch.maxRecords(), fetch.maxBytes()),
                        fetch.epoch(), authorityBefore.highWatermark(), leader.logEndOffset()));
            })) {
                Endpoint endpoint = scriptedLeader.start();
                PartitionLog follower = cluster.partitionLog(2, tp);
                Map<String, String> followerBefore = diskImage(logDirectory(cluster, 2, tp));
                CourseException stale = reconcileFailure(
                        cluster, 2, tp, follower, endpoint, authorityBefore.epoch() - 1);
                assertEquals(ErrorCode.FENCED_EPOCH, stale.code());
                assertEquals(followerBefore, diskImage(logDirectory(cluster, 2, tp)));
                assertEquals(authorityBefore, cluster.snapshot(tp));

                Map<String, String> leaderBefore = diskImage(logDirectory(cluster, 1, tp));
                CourseException self = reconcileFailure(cluster, 1, tp, leader, endpoint, authorityBefore.epoch());
                assertEquals(ErrorCode.INVALID_REQUEST, self.code());
                assertEquals(leaderBefore, diskImage(logDirectory(cluster, 1, tp)));
                assertEquals(authorityBefore, cluster.snapshot(tp));

                CourseException unassigned = reconcileFailure(
                        cluster, 4, tp, follower, endpoint, authorityBefore.epoch());
                assertEquals(ErrorCode.INVALID_REQUEST, unassigned.code());
                assertEquals(followerBefore, diskImage(logDirectory(cluster, 2, tp)));
                assertEquals(authorityBefore, cluster.snapshot(tp));
                assertEquals(0, rpcRequests.get(), "invalid reconciliation entries must fail before network fetch");
            }
        }
    }

    @Test
    void rpcTimeoutDoesNotPublishProofOrTruncateAnUnverifiedLocalTail() throws Exception {
        TopicPartition tp = new TopicPartition("step26timeout", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(3, checkpoint(cluster, tp, prefix()));
            PartitionLog local = cluster.partitionLog(2, tp);
            assertEquals(new AppendResult(3, 4), local.append(List.of(record("local-uncommitted-tail", 40))));
            List<LogRecord> localBefore = local.read(0, 10, 65_536);
            Map<String, String> diskBefore = diskImage(logDirectory(cluster, 2, tp));
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            CountDownLatch requestReceived = new CountDownLatch(1);
            CountDownLatch releaseResponse = new CountDownLatch(1);

            try (BrokerServer scriptedLeader = new BrokerServer("127.0.0.1", 0, request -> {
                requestReceived.countDown();
                try {
                    releaseResponse.await(3, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
                return Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "scripted timeout response");
            })) {
                Endpoint endpoint = scriptedLeader.start();
                try {
                    try (RpcClient client = new RpcClient(endpoint, 300)) {
                        ReplicaReconciler reconciler = new ReplicaReconciler(
                                2, tp, local, client, cluster.authority());
                        CourseException timeout = assertThrows(CourseException.class, () -> reconciler.reconcile(0));
                        assertEquals(ErrorCode.REQUEST_TIMEOUT, timeout.code());
                        assertTrue(reconciler.recoveryProof().isEmpty(), "a timed-out scan must not publish proof");
                    }
                    assertTrue(requestReceived.await(2, TimeUnit.SECONDS), "the real RPC request must reach the server");
                    assertEquals(localBefore, local.read(0, 10, 65_536));
                    assertEquals(4, local.logEndOffset(), "a timeout must not truncate the local-only tail");
                    assertEquals(diskBefore, diskImage(logDirectory(cluster, 2, tp)));
                    assertEquals(authorityBefore, cluster.snapshot(tp));
                } finally {
                    releaseResponse.countDown();
                }
            }
        }
    }

    @Test
    void malformedRpcReplyDoesNotPublishProofOrTruncateAnUnverifiedLocalTail() throws IOException {
        TopicPartition tp = new TopicPartition("step26malformed", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(3, checkpoint(cluster, tp, prefix()));
            PartitionLog local = cluster.partitionLog(2, tp);
            assertEquals(new AppendResult(3, 4), local.append(List.of(record("local-uncommitted-tail", 40))));
            List<LogRecord> localBefore = local.read(0, 10, 65_536);
            Map<String, String> diskBefore = diskImage(logDirectory(cluster, 2, tp));
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            AtomicInteger rpcRequests = new AtomicInteger();

            try (BrokerServer scriptedLeader = new BrokerServer("127.0.0.1", 0, request -> {
                rpcRequests.incrementAndGet();
                Messages.ReplicaFetchRequest fetch = (Messages.ReplicaFetchRequest) request;
                return Messages.Reply.success(new Messages.ReplicaFetchBody(
                        List.of(new LogRecord(fetch.fetchOffset() + 1, record("not-the-requested-offset", 900))),
                        fetch.epoch(), authorityBefore.highWatermark(), 3));
            })) {
                Endpoint endpoint = scriptedLeader.start();
                try (RpcClient client = new RpcClient(endpoint, 3_000)) {
                    ReplicaReconciler reconciler = new ReplicaReconciler(
                            2, tp, local, client, cluster.authority());
                    CourseException corrupt = assertThrows(CourseException.class, () -> reconciler.reconcile(0));
                    assertEquals(ErrorCode.CORRUPT_RECORD, corrupt.code());
                    assertEquals(1, rpcRequests.get(), "the malformed reply must cross the real RPC boundary");
                    assertTrue(reconciler.recoveryProof().isEmpty(), "an invalid response must not publish proof");
                    assertEquals(localBefore, local.read(0, 10, 65_536));
                    assertEquals(4, local.logEndOffset(), "an invalid response must not truncate the local-only tail");
                    assertEquals(diskBefore, diskImage(logDirectory(cluster, 2, tp)));
                    assertEquals(authorityBefore, cluster.snapshot(tp));
                }
            }
        }
    }
    @Test
    void sameLeoLocalRewriteDuringRecoveryIsDetectedBeforeAnyRepairOrProofPublication() throws Exception {
        TopicPartition tp = new TopicPartition("step26-version-race", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(3, checkpoint(cluster, tp, prefix()));
            PartitionLog leader = cluster.partitionLog(1, tp);
            PartitionLog local = cluster.partitionLog(2, tp);
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            Map<String, String> diskBefore = diskImage(logDirectory(cluster, 2, tp));
            CountDownLatch requestReceived = new CountDownLatch(1);
            CountDownLatch releaseResponse = new CountDownLatch(1);

            try (BrokerServer scriptedLeader = new BrokerServer("127.0.0.1", 0, request -> {
                if (!(request instanceof Messages.ReplicaFetchRequest fetch))
                    return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "expected replica fetch");
                requestReceived.countDown();
                try {
                    if (!releaseResponse.await(5, TimeUnit.SECONDS))
                        return Messages.Reply.failure(ErrorCode.REQUEST_TIMEOUT, "test did not release recovery response");
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return Messages.Reply.failure(ErrorCode.REQUEST_TIMEOUT, "recovery response interrupted");
                }
                return Messages.Reply.success(new Messages.ReplicaFetchBody(
                        leader.read(fetch.fetchOffset(), fetch.maxRecords(), fetch.maxBytes()),
                        fetch.epoch(), authorityBefore.highWatermark(), leader.logEndOffset()));
            })) {
                Endpoint endpoint = scriptedLeader.start();
                try (RpcClient client = new RpcClient(endpoint, 5_000)) {
                    ReplicaReconciler reconciler = new ReplicaReconciler(2, tp, local, client, cluster.authority());
                    AtomicReference<Throwable> failure = new AtomicReference<>();
                    Thread scan = new Thread(() -> {
                        try {
                            reconciler.reconcile(0);
                        } catch (Throwable thrown) {
                            failure.set(thrown);
                        }
                    }, "step26-local-mutation-scan");
                    scan.setDaemon(true);
                    scan.start();
                    try {
                        assertTrue(requestReceived.await(2, TimeUnit.SECONDS), "recovery must be paused in real RPC");
                        List<RecordData> replacement = List.of(
                                record("replacement-zero", 101), record("replacement-one", 102),
                                record("replacement-two", 103));
                        local.truncateTo(0);
                        local.append(replacement);
                        List<LogRecord> changedRecords = expectedRecords(replacement);
                        Map<String, String> changedDisk = diskImage(logDirectory(cluster, 2, tp));
                        assertEquals(3, local.logEndOffset(), "the replacement retains the same LEO");
                        assertTrue(!diskBefore.equals(changedDisk), "same-LEO replacement changes durable bytes");

                        releaseResponse.countDown();
                        scan.join(3_000);
                        assertTrue(!scan.isAlive(), "reconciliation worker must finish");
                        assertTrue(failure.get() instanceof CourseException, "expected REQUEST_TIMEOUT, got " + failure.get());
                        assertEquals(ErrorCode.REQUEST_TIMEOUT, ((CourseException) failure.get()).code());
                        assertEquals(changedRecords, local.read(0, 10, 65_536),
                                "a concurrent same-LEO rewrite must not be replaced by recovery");
                        assertEquals(changedDisk, diskImage(logDirectory(cluster, 2, tp)));
                        assertEquals(authorityBefore, cluster.snapshot(tp));
                        assertTrue(reconciler.recoveryProof().isEmpty());
                    } finally {
                        releaseResponse.countDown();
                        scan.join(3_000);
                    }
                }
            }
        }
    }


    private static long checkpoint(ClusterHarness cluster, TopicPartition tp, List<RecordData> records) {
        Messages.Reply reply = produce(cluster, tp, 1, 0, Acks.LEADER, records);
        assertEquals(ErrorCode.NONE, reply.error());
        assertEquals(new AppendResult(0, records.size()), ((Messages.ProduceBody) reply.body()).result());
        for (int brokerId : List.of(2, 3)) {
            assertEquals(records.size(), cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
            cluster.tracker(tp).report(brokerId, 0, records.size());
        }
        assertEquals(records.size(), cluster.tracker(tp).highWatermark());
        return cluster.partitionLog(1, tp).logEndOffset();
    }

    private static Messages.Reply produce(ClusterHarness cluster, TopicPartition tp, int brokerId, int epoch,
                                          Acks acks, List<RecordData> records) {
        try (RpcClient client = new RpcClient(cluster.endpoint(brokerId), 3_000)) {
            return client.call(new Messages.ProduceRequest(tp, epoch, acks, 2_000, records));
        }
    }

    private static CourseException reconcileFailure(ClusterHarness cluster, int brokerId, TopicPartition tp,
                                                     PartitionLog local, Endpoint endpoint, int epoch) {
        try (RpcClient client = new RpcClient(endpoint, 1_000)) {
            ReplicaReconciler reconciler = new ReplicaReconciler(
                    brokerId, tp, local, client, cluster.authority());
            return assertThrows(CourseException.class, () -> reconciler.reconcile(epoch));
        }
    }

    private static Path logDirectory(ClusterHarness cluster, int brokerId, TopicPartition tp) {
        return cluster.catalog(brokerId).root().resolve(tp.topic()).resolve(Integer.toString(tp.partition()));
    }

    private static Map<String, String> diskImage(Path directory) throws IOException {
        Map<String, String> image = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(directory)) {
            for (Path file : paths.filter(Files::isRegularFile).toList()) {
                image.put(directory.relativize(file).toString(), HexFormat.of().formatHex(Files.readAllBytes(file)));
            }
        }
        return Map.copyOf(image);
    }

    private static List<LogRecord> expectedRecords(List<RecordData> records) {
        List<LogRecord> expected = new ArrayList<>(records.size());
        for (int offset = 0; offset < records.size(); offset++)
            expected.add(new LogRecord(offset, records.get(offset)));
        return List.copyOf(expected);
    }

    private static List<RecordData> records(int count) {
        List<RecordData> records = new ArrayList<>(count);
        for (int index = 0; index < count; index++)
            records.add(record("record-" + index, 1_000L + index * 17L));
        return List.copyOf(records);
    }

    private static List<RecordData> prefix() {
        return List.of(record("zero", 10), record("one", 11), record("two", 12));
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
