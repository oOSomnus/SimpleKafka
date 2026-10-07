package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicaState;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.replication.FollowerReplicator;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;


class Step21Test {
    @Test
    void discardsAReplyWhenReplicaEpochOrRoleChangesDuringTheTcpRequest() throws Exception {
        TopicPartition tp = new TopicPartition("in-flight", 0);
        List<RoleTransition> transitions = List.of(
                new RoleTransition("epoch-change", 3, false, true, ErrorCode.FENCED_EPOCH),
                new RoleTransition("becomes-leader", 0, true, true, ErrorCode.NOT_LEADER),
                new RoleTransition("goes-offline", 0, false, false, ErrorCode.NOT_LEADER));

        for (RoleTransition transition : transitions) {
            Path directory = root.resolve(transition.name());
            CountDownLatch requestReceived = new CountDownLatch(1);
            CountDownLatch releaseReply = new CountDownLatch(1);
            BrokerServer server = new BrokerServer("127.0.0.1", 0, request -> {
                if (!(request instanceof Messages.ReplicaFetchRequest))
                    return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "expected replica fetch");
                requestReceived.countDown();
                try {
                    if (!releaseReply.await(5, TimeUnit.SECONDS))
                        return Messages.Reply.failure(ErrorCode.REQUEST_TIMEOUT, "test reply gate expired");
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                    return Messages.Reply.failure(ErrorCode.REQUEST_TIMEOUT, "test reply gate interrupted");
                }
                return Messages.Reply.success(new Messages.ReplicaFetchBody(
                        List.of(new LogRecord(0, record("reply", 10))), 0, 0, 1));
            });

            try (server; PartitionLog log = new PartitionLog(directory, 96, 1)) {
                var endpoint = server.start();
                Path logFile = directory.resolve("00000000000000000000.log");
                byte[] before = Files.readAllBytes(logFile);
                ReplicaState state = new ReplicaState(2, 0, false, true);
                ExecutorService worker = Executors.newSingleThreadExecutor();
                try (RpcClient client = new RpcClient(endpoint, 2_000)) {
                    Future<Integer> poll = worker.submit(() ->
                            new FollowerReplicator(2, tp, log, client, state).pollOnce(10, 4_096));
                    try {
                        assertTrue(requestReceived.await(2, TimeUnit.SECONDS),
                                transition.name() + ": leader did not receive the fetch");
                        state.update(transition.epoch(), transition.leader(), transition.online());
                        releaseReply.countDown();

                        ExecutionException failure = assertThrows(ExecutionException.class,
                                () -> poll.get(2, TimeUnit.SECONDS), transition.name());
                        CourseException cause = assertInstanceOf(CourseException.class, failure.getCause());
                        assertEquals(transition.expectedError(), cause.code(), transition.name());
                        assertEquals(0, log.logEndOffset(), transition.name());
                        assertEquals(List.of(), log.read(0, 10, 4_096), transition.name());
                        assertArrayEquals(before, Files.readAllBytes(logFile), transition.name());
                    } finally {
                        releaseReply.countDown();
                        worker.shutdownNow();
                        assertTrue(worker.awaitTermination(2, TimeUnit.SECONDS),
                                transition.name() + ": poll worker did not terminate");
                    }
                }
            }
        }
    }

    private record RoleTransition(String name, int epoch, boolean leader, boolean online, ErrorCode expectedError) {}
    @TempDir Path root;

    @Test
    void replicaFetchIncludesUncommittedLeaderTailAndReturnsEmptyAtLeo() {
        TopicPartition tp = new TopicPartition("step21-fetch", 0);
        try (ClusterHarness cluster = new ClusterHarness(root)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            assertEquals(0, authorityBefore.highWatermark());

            List<RecordData> batch = List.of(
                    record("tail-a", 11), record("tail-b", 12), record("tail-c", 13));
            var append = cluster.partitionLog(1, tp).append(batch);
            assertEquals(0, append.firstOffset());
            assertEquals(3, append.nextOffset());
            assertEquals(0, cluster.snapshot(tp).highWatermark(),
                    "a direct leader-log append remains above the authority high watermark");

            List<LogRecord> expected = List.of(
                    new LogRecord(0, record("tail-a", 11)),
                    new LogRecord(1, record("tail-b", 12)),
                    new LogRecord(2, record("tail-c", 13)));
            try (RpcClient client = cluster.client(1, 2_000)) {
                Messages.Reply reply = client.call(
                        new Messages.ReplicaFetchRequest(tp, 2, 0, 0, 10, 16_384, false));
                assertEquals(ErrorCode.NONE, reply.error());
                Messages.ReplicaFetchBody body = assertInstanceOf(Messages.ReplicaFetchBody.class, reply.body());
                assertEquals(expected, body.records(),
                        "replica fetch includes the complete leader suffix even though HW is still zero");
                assertEquals(0, body.highWatermark());
                assertEquals(3, body.leaderLogEndOffset());
                assertEquals(0, body.epoch());

                Messages.Reply emptyReply = client.call(
                        new Messages.ReplicaFetchRequest(tp, 2, 0, 3, 10, 16_384, false));
                assertEquals(ErrorCode.NONE, emptyReply.error());
                Messages.ReplicaFetchBody emptyBody =
                        assertInstanceOf(Messages.ReplicaFetchBody.class, emptyReply.body());
                assertEquals(List.of(), emptyBody.records());
                assertEquals(3, emptyBody.leaderLogEndOffset());
                assertEquals(0, emptyBody.highWatermark());
            }
            assertEquals(authorityBefore, cluster.snapshot(tp),
                    "serving replica fetches must not report follower progress or advance authority state");
        }
    }

    @Test
    void followerCopySurvivesEmptyPollsAndCatalogReopen() throws Exception {
        AtomicLong now = new AtomicLong(10);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step21-copy", 0);
        List<LogRecord> expected = List.of(
                new LogRecord(0, record("first", 11)),
                new LogRecord(1, record("second", 12)),
                new LogRecord(2, record("third", 13)));
        Path followerFile = segmentFile(root, 2, tp);
        byte[] copiedBytes;

        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            cluster.partitionLog(1, tp).append(List.of(
                    record("first", 11), record("second", 12), record("third", 13)));

            List<Integer> copiedCounts = new ArrayList<>();
            for (int index = 0; index < 3; index++)
                copiedCounts.add(cluster.replicateOnce(2, tp, 1, 16_384));
            assertEquals(List.of(1, 1, 1), copiedCounts,
                    "one-record real TCP polls must advance the follower through successive offsets");
            assertEquals(3, cluster.partitionLog(2, tp).logEndOffset());
            assertEquals(expected, cluster.partitionLog(2, tp).read(0, 10, 16_384));
            assertEquals(expected, RecordBytes.readRecords(followerFile));

            copiedBytes = Files.readAllBytes(followerFile);
            assertEquals(0, cluster.replicateOnce(2, tp, 1, 16_384));
            assertArrayEquals(copiedBytes, Files.readAllBytes(followerFile),
                    "an empty poll at follower LEO must not rewrite its log bytes");
            assertEquals(0, cluster.replicateOnce(2, tp, 1, 16_384));
            assertArrayEquals(copiedBytes, Files.readAllBytes(followerFile),
                    "repeated empty polls must keep the copied disk image unchanged");
            assertEquals(authorityBefore, cluster.snapshot(tp),
                    "replication polling itself must not report progress to authority");
        }

        try (ClusterHarness reopened = new ClusterHarness(root, clock)) {
            reopened.start();
            reopened.createTopic(tp.topic(), 1, 2);
            assertEquals(3, reopened.partitionLog(2, tp).logEndOffset());
            assertEquals(expected, reopened.partitionLog(2, tp).read(0, 10, 16_384));
            assertEquals(expected, RecordBytes.readRecords(followerFile),
                    "the copied records and offsets must remain readable after catalog reopen");
            assertArrayEquals(copiedBytes, Files.readAllBytes(followerFile));
        }
    }

    @Test
    void rejectedReplicaFetchRequestsPreserveDiskAndAuthority() throws Exception {
        TopicPartition tp = new TopicPartition("step21-rejections", 0);
        try (ClusterHarness cluster = new ClusterHarness(root);
             BrokerServer unassignedBroker = new BrokerServer("127.0.0.1", 0,
                     request -> Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "unexpected request"))) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            cluster.authority().registerBroker(4, unassignedBroker.start());
            cluster.partitionLog(1, tp).append(List.of(record("leader", 21)));

            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);
            List<ReplicaDiskImage> diskBefore = captureReplicaLogs(root, cluster, tp);
            List<ReplicaFetchFailure> failures = List.of(
                    new ReplicaFetchFailure("stale epoch", 1,
                            new Messages.ReplicaFetchRequest(tp, 2, 1, 0, 10, 4_096, false),
                            ErrorCode.FENCED_EPOCH),
                    new ReplicaFetchFailure("unknown target partition", 1,
                            new Messages.ReplicaFetchRequest(new TopicPartition(tp.topic(), 1),
                                    2, 0, 0, 10, 4_096, false),
                            ErrorCode.UNKNOWN_TOPIC_OR_PARTITION),
                    new ReplicaFetchFailure("request served by nonleader broker", 3,
                            new Messages.ReplicaFetchRequest(tp, 2, 0, 0, 10, 4_096, false),
                            ErrorCode.NOT_LEADER),
                    new ReplicaFetchFailure("zero record limit", 1,
                            new Messages.ReplicaFetchRequest(tp, 2, 0, 0, 0, 4_096, false),
                            ErrorCode.INVALID_REQUEST),
                    new ReplicaFetchFailure("zero byte limit", 1,
                            new Messages.ReplicaFetchRequest(tp, 2, 0, 0, 10, 0, false),
                            ErrorCode.INVALID_REQUEST),
                    new ReplicaFetchFailure("offset beyond leader LEO", 1,
                            new Messages.ReplicaFetchRequest(tp, 2, 0, 2, 10, 4_096, false),
                            ErrorCode.OFFSET_OUT_OF_RANGE),
                    new ReplicaFetchFailure("negative replica id", 1,
                            new Messages.ReplicaFetchRequest(tp, -1, 0, 0, 10, 4_096, false),
                            ErrorCode.INVALID_REQUEST),
                    new ReplicaFetchFailure("unassigned replica id", 1,
                            new Messages.ReplicaFetchRequest(tp, 4, 0, 0, 10, 4_096, false),
                            ErrorCode.INVALID_REQUEST));

            for (ReplicaFetchFailure failure : failures)
                assertRejectedFetch(cluster, tp, failure, authorityBefore, diskBefore, root);
        }
    }

    @Test
    void followerAheadOfLeaderIsRejectedWithoutTruncatingItsLog() throws Exception {
        TopicPartition tp = new TopicPartition("step21-ahead", 0);
        try (ClusterHarness cluster = new ClusterHarness(root)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            PartitionLog follower = cluster.partitionLog(2, tp);
            follower.append(List.of(record("follower-only", 31)));
            Path followerFile = segmentFile(root, 2, tp);
            byte[] followerBytes = Files.readAllBytes(followerFile);
            ReplicationSnapshot authorityBefore = cluster.snapshot(tp);

            CourseException failure = assertThrows(CourseException.class,
                    () -> cluster.replicateOnce(2, tp, 10, 4_096));
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, failure.code());
            assertEquals(1, follower.logEndOffset());
            assertEquals(List.of(new LogRecord(0, record("follower-only", 31))),
                    follower.read(0, 10, 4_096));
            assertArrayEquals(followerBytes, Files.readAllBytes(followerFile));
            assertEquals(0, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(authorityBefore, cluster.snapshot(tp));
        }
    }

    @Test
    void malformedRealTcpReplicaFetchRepliesAreRejectedBeforeAppend() throws Exception {
        TopicPartition tp = new TopicPartition("step21-scripted", 0);
        List<ScriptedReply> replies = List.of(
                new ScriptedReply("wrong-epoch",
                        new Messages.ReplicaFetchBody(
                                List.of(new LogRecord(0, record("stale", 40))), 1, 0, 1),
                        ErrorCode.FENCED_EPOCH),
                new ScriptedReply("wrong-first-offset",
                        new Messages.ReplicaFetchBody(
                                List.of(new LogRecord(1, record("wrong-first", 41))), 0, 0, 2),
                        ErrorCode.CORRUPT_RECORD),
                new ScriptedReply("discontinuous-records",
                        new Messages.ReplicaFetchBody(List.of(
                                new LogRecord(0, record("first", 42)),
                                new LogRecord(2, record("gap", 43))), 0, 0, 3),
                        ErrorCode.CORRUPT_RECORD));

        for (ScriptedReply scriptedReply : replies) {
            TopicPartition scriptedTp = new TopicPartition(tp.topic() + "-" + scriptedReply.name(), 0);
            Path directory = root.resolve(scriptedReply.name());
            BrokerServer server = new BrokerServer("127.0.0.1", 0, request -> {
                if (!(request instanceof Messages.ReplicaFetchRequest fetch)
                        || !fetch.tp().equals(scriptedTp) || fetch.brokerId() != 2
                        || fetch.epoch() != 0 || fetch.fetchOffset() != 0)
                    return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "unexpected replica fetch request");
                return Messages.Reply.success(scriptedReply.body());
            });

            try (server; PartitionLog local = new PartitionLog(directory, 96, 1)) {
                var endpoint = server.start();
                Path logFile = segmentFile(directory);
                byte[] before = Files.readAllBytes(logFile);
                ReplicaState state = new ReplicaState(2, 0, false, true);
                try (RpcClient client = new RpcClient(endpoint, 2_000)) {
                    CourseException failure = assertThrows(CourseException.class,
                            () -> new FollowerReplicator(2, scriptedTp, local, client, state)
                                    .pollOnce(10, 4_096),
                            scriptedReply.name());
                    assertEquals(scriptedReply.expectedError(), failure.code(), scriptedReply.name());
                }
                assertEquals(0, local.logEndOffset(), scriptedReply.name());
                assertEquals(List.of(), local.read(0, 10, 4_096), scriptedReply.name());
                assertArrayEquals(before, Files.readAllBytes(logFile), scriptedReply.name());
            }
        }
    }

    private record ReplicaFetchFailure(String name, int serverBrokerId,
                                       Messages.ReplicaFetchRequest request, ErrorCode expectedError) {}

    private record ReplicaDiskImage(int brokerId, long logEndOffset, byte[] logBytes) {}

    private record ScriptedReply(String name, Messages.ReplicaFetchBody body, ErrorCode expectedError) {}

    private static void assertRejectedFetch(ClusterHarness cluster, TopicPartition tp,
                                            ReplicaFetchFailure failure,
                                            ReplicationSnapshot authorityBefore,
                                            List<ReplicaDiskImage> diskBefore, Path root) throws Exception {
        try (RpcClient client = cluster.client(failure.serverBrokerId(), 2_000)) {
            Messages.Reply reply = client.call(failure.request());
            assertEquals(failure.expectedError(), reply.error(), failure.name());
        }
        assertEquals(authorityBefore, cluster.snapshot(tp), failure.name());
        assertReplicaLogsUnchanged(cluster, tp, diskBefore, root, failure.name());
    }

    private static List<ReplicaDiskImage> captureReplicaLogs(Path root, ClusterHarness cluster,
                                                             TopicPartition tp) throws Exception {
        List<ReplicaDiskImage> images = new ArrayList<>();
        for (int brokerId : ClusterHarness.BROKER_IDS) {
            Path logFile = segmentFile(root, brokerId, tp);
            images.add(new ReplicaDiskImage(brokerId, cluster.partitionLog(brokerId, tp).logEndOffset(),
                    Files.readAllBytes(logFile)));
        }
        return List.copyOf(images);
    }

    private static void assertReplicaLogsUnchanged(ClusterHarness cluster, TopicPartition tp,
                                                   List<ReplicaDiskImage> before, Path root,
                                                   String message) throws Exception {
        for (ReplicaDiskImage image : before) {
            assertEquals(image.logEndOffset(), cluster.partitionLog(image.brokerId(), tp).logEndOffset(),
                    message + " (broker " + image.brokerId() + " LEO)");
            assertArrayEquals(image.logBytes(), Files.readAllBytes(segmentFile(root, image.brokerId(), tp)),
                    message + " (broker " + image.brokerId() + " bytes)");
        }
    }

    private static Path segmentFile(Path root, int brokerId, TopicPartition tp) {
        return segmentFile(root.resolve("broker-" + brokerId).resolve(tp.topic())
                .resolve(Integer.toString(tp.partition())));
    }

    private static Path segmentFile(Path partitionDirectory) {
        return partitionDirectory.resolve("00000000000000000000.log");
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
