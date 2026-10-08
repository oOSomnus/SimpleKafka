package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step24Test {
    @TempDir Path root;

    @Test
    void pendingAllAckLeavesFetchAtOldHwAllowsLeaderProduceAndCompletesAfterTcpReplication() throws Exception {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step24", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Messages.Reply prefixReply = produce(cluster, tp, 0, Acks.LEADER,
                    List.of(record("committed-prefix", 1)));
            assertEquals(ErrorCode.NONE, prefixReply.error());
            assertEquals(new AppendResult(0, 1), ((Messages.ProduceBody) prefixReply.body()).result());

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(1, cluster.tracker(tp).highWatermark());
            assertEquals(List.of("committed-prefix"), values(fetch(cluster, tp, 0).records()));
            assertEquals(List.of(), fetch(cluster, tp, 1).records());

            AsyncProduce allPending = startProduce(cluster, tp, Acks.ALL,
                    List.of(record("all-pending", 2)), 5_000, 10_000);
            try {
                awaitLeaderEnd(cluster, tp, 2, allPending.thread());
                assertEquals(1, cluster.tracker(tp).highWatermark());
                assertFalse(allPending.completed().await(200, TimeUnit.MILLISECONDS));

                Messages.Reply leaderReply = produce(cluster, tp, 0, Acks.LEADER,
                        List.of(record("leader-during-wait", 3)));
                assertEquals(ErrorCode.NONE, leaderReply.error());
                assertEquals(new AppendResult(2, 3), ((Messages.ProduceBody) leaderReply.body()).result());
                assertEquals(3, cluster.partitionLog(1, tp).logEndOffset());
                assertEquals(1, cluster.tracker(tp).highWatermark());

                Messages.FetchBody duringWait = fetch(cluster, tp, 0);
                assertEquals(1, duringWait.highWatermark());
                assertEquals(3, duringWait.logEndOffset());
                assertEquals(List.of("committed-prefix"), values(duringWait.records()));
                assertEquals(List.of(), fetch(cluster, tp, 1).records());

                for (int brokerId : List.of(2, 3)) {
                    assertEquals(2, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                    cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
                }
                assertEquals(3, cluster.tracker(tp).highWatermark());
                Messages.Reply allReply = awaitSuccessfulProduce(allPending);
                assertEquals(new AppendResult(1, 2), ((Messages.ProduceBody) allReply.body()).result());
                assertEquals(List.of("committed-prefix", "all-pending", "leader-during-wait"),
                        values(fetch(cluster, tp, 0).records()));
            } finally {
                releaseProducer(cluster, tp, allPending.thread());
            }

            AsyncProduce hugeTimeout = startProduce(cluster, tp, Acks.ALL,
                    List.of(record("saturated-deadline", 4)), Long.MAX_VALUE, 10_000);
            try {
                awaitLeaderEnd(cluster, tp, 4, hugeTimeout.thread());
                assertEquals(3, cluster.tracker(tp).highWatermark());
                assertFalse(hugeTimeout.completed().await(200, TimeUnit.MILLISECONDS),
                        "Long.MAX_VALUE timeout must not overflow into an immediate timeout");
                Messages.FetchBody beforeHugeReplication = fetch(cluster, tp, 3);
                assertEquals(3, beforeHugeReplication.highWatermark());
                assertEquals(4, beforeHugeReplication.logEndOffset());
                assertEquals(List.of(), beforeHugeReplication.records());

                for (int brokerId : List.of(2, 3)) {
                    assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                    cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
                }
                assertEquals(4, cluster.tracker(tp).highWatermark());
                Messages.Reply hugeReply = awaitSuccessfulProduce(hugeTimeout);
                assertEquals(new AppendResult(3, 4), ((Messages.ProduceBody) hugeReply.body()).result());
                assertEquals(List.of("saturated-deadline"), values(fetch(cluster, tp, 3).records()));
            } finally {
                releaseProducer(cluster, tp, hugeTimeout.thread());
            }
        }
    }

    @Test
    void fetchHonorsHwLeoAndWholeRecordByteBudgetsAndRejectsInvalidRequests() {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step24-limits", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Messages.Reply append = produce(cluster, tp, 0, Acks.LEADER,
                    List.of(record("", 1), record("", 2), record("", 3), record("", 4)));
            assertEquals(ErrorCode.NONE, append.error());
            assertEquals(new AppendResult(0, 4), ((Messages.ProduceBody) append.body()).result());

            for (int brokerId : List.of(2, 3)) {
                assertEquals(2, cluster.replicator(brokerId, tp).pollOnce(2, 4_096));
                cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(2, cluster.tracker(tp).highWatermark());
            assertEquals(4, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(List.of(), offsets(fetch(cluster, tp, 0, 2, 31).records()));
            assertEquals(List.of(0L), offsets(fetch(cluster, tp, 0, 2, 32).records()));
            assertEquals(List.of(0L), offsets(fetch(cluster, tp, 0, 2, 63).records()));
            assertEquals(List.of(0L, 1L), offsets(fetch(cluster, tp, 0, 2, 64).records()));
            assertEquals(List.of(0L), offsets(fetch(cluster, tp, 0, 1, 64).records()));
            assertEquals(List.of(1L), offsets(fetch(cluster, tp, 1, 10, 64).records()));
            Messages.FetchBody atHighWatermark = fetch(cluster, tp, 2);
            assertEquals(2, atHighWatermark.highWatermark());
            assertEquals(4, atHighWatermark.logEndOffset());
            assertEquals(List.of(), atHighWatermark.records());
            Messages.FetchBody atLogEnd = fetch(cluster, tp, 4);
            assertEquals(2, atLogEnd.highWatermark());
            assertEquals(4, atLogEnd.logEndOffset());
            assertEquals(List.of(), atLogEnd.records());
            assertEquals(List.of(), fetch(cluster, tp, 3).records());
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, fetchReply(cluster, tp, 5, 10, 64).error());

            ReplicationSnapshot before = cluster.tracker(tp).snapshot();
            List<Long> beforeLEOs = logEnds(cluster, tp);
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs, ErrorCode.FENCED_EPOCH,
                    produce(cluster, tp, 1, Acks.LEADER, List.of(record("bad-epoch", 4))));
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs, ErrorCode.INVALID_REQUEST,
                    produce(cluster, tp, 0, Acks.LEADER, List.of()));
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs, ErrorCode.INVALID_REQUEST,
                    fetchReply(cluster, tp, 0, 0, 64));
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs, ErrorCode.INVALID_REQUEST,
                    fetchReply(cluster, tp, 0, 10, 0));
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs, ErrorCode.INVALID_REQUEST,
                    fetchReply(cluster, tp, -1, 10, 64));
        }
    }

    @Test
    void retainedLogStartIsEnforcedOverTcpWithoutChangingReplicationState() {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step24-retained", 0);
        List<RecordData> batch = List.of(
                new RecordData(null, new byte[600_000], 10),
                new RecordData(null, new byte[600_000], 11));
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Messages.Reply appended = produce(cluster, tp, 0, Acks.LEADER, batch);
            assertEquals(ErrorCode.NONE, appended.error());
            assertEquals(new AppendResult(0, 2), ((Messages.ProduceBody) appended.body()).result());
            assertEquals(2, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(0, cluster.tracker(tp).highWatermark());
            Path partitionDirectory = root.resolve("broker-1").resolve(tp.topic()).resolve("0");
            Path firstSegment = partitionDirectory.resolve("00000000000000000000.log");
            Path retainedSegment = partitionDirectory.resolve("00000000000000000001.log");
            assertTrue(Files.exists(firstSegment));
            assertTrue(Files.exists(retainedSegment));

            assertEquals(1, cluster.partitionLog(1, tp).deleteBefore(1));
            assertEquals(1, cluster.partitionLog(1, tp).logStartOffset());
            assertFalse(Files.exists(firstSegment));
            assertTrue(Files.exists(retainedSegment));
            ReplicationSnapshot before = cluster.tracker(tp).snapshot();
            List<Long> beforeLEOs = logEnds(cluster, tp);
            List<LogRecord> retainedRecords = cluster.partitionLog(1, tp).read(1, 1, 700_000);

            Messages.Reply beforeStart = fetchReply(cluster, tp, 0, 1, 1_000_000);
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs,
                    ErrorCode.OFFSET_OUT_OF_RANGE, beforeStart);
            assertEquals(retainedRecords, cluster.partitionLog(1, tp).read(1, 1, 700_000));

            Messages.Reply atRetainedStart = fetchReply(cluster, tp, 1, 10, 1_000_000);
            assertEquals(ErrorCode.NONE, atRetainedStart.error());
            Messages.FetchBody empty = (Messages.FetchBody) atRetainedStart.body();
            assertEquals(List.of(), empty.records());
            assertEquals(1, empty.logStartOffset());
            assertEquals(2, empty.logEndOffset());
            assertEquals(0, empty.highWatermark());
            assertEquals(0, empty.epoch());
            assertEquals(before, cluster.tracker(tp).snapshot());
            assertEquals(beforeLEOs, logEnds(cluster, tp));
            assertEquals(retainedRecords, cluster.partitionLog(1, tp).read(1, 1, 700_000));

            Messages.Reply futureEpoch;
            try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
                futureEpoch = client.call(new Messages.FetchRequest(tp, 1, 1, 10, 1_000_000, null));
            }
            assertUnchangedAfterReply(cluster, tp, before, beforeLEOs, ErrorCode.FENCED_EPOCH, futureEpoch);
            assertEquals(retainedRecords, cluster.partitionLog(1, tp).read(1, 1, 700_000));
        }
    }

    @Test
    void allPrecheckDoesNotAppendAndZeroTimeoutRetainsAnInvisibleAppendUntilReplication() {
        AtomicLong now = new AtomicLong(1_000);
        TimeSource clock = now::get;
        TopicPartition insufficientTp = new TopicPartition("step24-min-isr", 0);
        try (ClusterHarness cluster = new ClusterHarness(root.resolve("min-isr"), clock)) {
            cluster.start();
            cluster.createTopic(insufficientTp.topic(), 1, 3);
            now.set(2_000);
            assertEquals(Set.of(2, 3), cluster.tracker(insufficientTp).expireLagging());
            ReplicationSnapshot before = cluster.tracker(insufficientTp).snapshot();
            List<Long> beforeLEOs = logEnds(cluster, insufficientTp);

            Messages.Reply rejected = produce(cluster, insufficientTp, 0, Acks.ALL,
                    List.of(record("not-appended", 1)));
            assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, rejected.error());
            assertUnchanged(cluster, insufficientTp, before, beforeLEOs);

            Messages.Reply leaderAck = produce(cluster, insufficientTp, 0, Acks.LEADER,
                    List.of(record("leader-ack", 2)));
            assertEquals(ErrorCode.NONE, leaderAck.error());
            assertEquals(new AppendResult(0, 1), ((Messages.ProduceBody) leaderAck.body()).result());
            assertEquals(0, cluster.tracker(insufficientTp).highWatermark());
            assertEquals(List.of(), fetch(cluster, insufficientTp, 0).records());
        }

        TopicPartition timeoutTp = new TopicPartition("step24-zero-timeout", 0);
        try (ClusterHarness cluster = new ClusterHarness(root.resolve("zero-timeout"), clock)) {
            cluster.start();
            cluster.createTopic(timeoutTp.topic(), 1, 2);
            ReplicationSnapshot before = cluster.tracker(timeoutTp).snapshot();
            Messages.Reply timedOut = produce(cluster, timeoutTp, 0, Acks.ALL,
                    List.of(record("retained-uncommitted", 3)), 0, 3_000);
            assertEquals(ErrorCode.REQUEST_TIMEOUT, timedOut.error());
            assertEquals(new ReplicationSnapshot(before.leaderId(), before.epoch(), before.replicas(), before.isr(),
                    0, java.util.Map.of(1, 1L, 2, 0L, 3, 0L)), cluster.tracker(timeoutTp).snapshot());
            assertEquals(1, cluster.partitionLog(1, timeoutTp).logEndOffset());
            assertEquals(0, cluster.tracker(timeoutTp).highWatermark());
            assertEquals(List.of(), fetch(cluster, timeoutTp, 0).records());

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, timeoutTp).pollOnce(10, 4_096));
                cluster.tracker(timeoutTp).report(brokerId, 0, cluster.partitionLog(brokerId, timeoutTp).logEndOffset());
            }
            assertEquals(1, cluster.tracker(timeoutTp).highWatermark());
            assertEquals(List.of("retained-uncommitted"), values(fetch(cluster, timeoutTp, 0).records()));
        }
    }

    @Test
    void producerAcksControlAdmissionVisibilityAndTimeoutWithoutDiscardingTheAppend() {
        AtomicLong now = new AtomicLong(1_000);
        TimeSource clock = now::get;
        TopicPartition insufficientTp = new TopicPartition("step24-producer-acks", 0);
        try (ClusterHarness cluster = new ClusterHarness(root.resolve("producer-acks"), clock)) {
            cluster.start();
            cluster.createTopic(insufficientTp.topic(), 1, 3);
            now.set(2_000);
            assertEquals(Set.of(2, 3), cluster.tracker(insufficientTp).expireLagging());

            try (SimpleProducer producer = new SimpleProducer(
                    new RpcClient(cluster.endpoint(1), 3_000), new Partitioner(), 1)) {
                producer.setAcks(Acks.ALL, 5_000);
                CourseException failure = assertThrows(CourseException.class,
                        () -> producer.send(insufficientTp.topic(), null,
                                "rejected-all".getBytes(StandardCharsets.UTF_8), 1));
                assertCourseFailure(failure, ErrorCode.NOT_ENOUGH_REPLICAS);
            }
            assertEquals(List.of(0L, 0L, 0L), logEnds(cluster, insufficientTp));
            assertEquals(0, cluster.tracker(insufficientTp).highWatermark());
            assertEquals(List.of(), fetch(cluster, insufficientTp, 0).records());

            try (SimpleProducer producer = new SimpleProducer(
                    new RpcClient(cluster.endpoint(1), 3_000), new Partitioner(), 1)) {
                producer.setAcks(Acks.LEADER, 5_000);
                producer.send(insufficientTp.topic(), null,
                        "leader-only".getBytes(StandardCharsets.UTF_8), 2);
                assertEquals(List.of(new ProduceReceipt(insufficientTp, 0, 1)), producer.flush());
            }
            assertEquals(List.of(1L, 0L, 0L), logEnds(cluster, insufficientTp));
            assertEquals(0, cluster.tracker(insufficientTp).highWatermark());
            assertEquals(List.of(), fetch(cluster, insufficientTp, 0).records());
            assertEquals(List.of("leader-only"), values(
                    cluster.partitionLog(1, insufficientTp).read(0, 1, 4_096)));
        }

        TopicPartition timeoutTp = new TopicPartition("step24-producer-timeout", 0);
        try (ClusterHarness cluster = new ClusterHarness(root.resolve("producer-timeout"), clock)) {
            cluster.start();
            cluster.createTopic(timeoutTp.topic(), 1, 2);
            try (SimpleProducer producer = new SimpleProducer(
                    new RpcClient(cluster.endpoint(1), 3_000), new Partitioner(), 1)) {
                producer.setAcks(Acks.ALL, 0);
                CourseException failure = assertThrows(CourseException.class,
                        () -> producer.send(timeoutTp.topic(), null,
                                "retained-uncommitted".getBytes(StandardCharsets.UTF_8), 3));
                assertCourseFailure(failure, ErrorCode.REQUEST_TIMEOUT);
            }
            assertEquals(List.of(1L, 0L, 0L), logEnds(cluster, timeoutTp));
            assertEquals(0, cluster.tracker(timeoutTp).highWatermark());
            assertEquals(List.of(), fetch(cluster, timeoutTp, 0).records());

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicateOnce(brokerId, timeoutTp, 10, 4_096));
                cluster.tracker(timeoutTp).report(
                        brokerId, 0, cluster.partitionLog(brokerId, timeoutTp).logEndOffset());
            }
            assertEquals(1, cluster.tracker(timeoutTp).highWatermark());
            assertEquals(List.of("retained-uncommitted"), values(fetch(cluster, timeoutTp, 0).records()));
        }
    }

    @Test
    void stoppingLeaderCompletesAnOutstandingAllAckRequestWithoutLeakingTheProducer() throws Exception {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step24shutdown", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Messages.Reply prefix = produce(cluster, tp, 0, Acks.LEADER, List.of(record("visible", 1)));
            assertEquals(ErrorCode.NONE, prefix.error());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(1, cluster.tracker(tp).highWatermark());

            AsyncProduce pending = startProduce(cluster, tp, Acks.ALL,
                    List.of(record("pending", 2)), 5_000, 10_000);
            try {
                awaitLeaderEnd(cluster, tp, 2, pending.thread());
                assertFalse(pending.completed().await(200, TimeUnit.MILLISECONDS));
                cluster.stopBroker(1);
                assertTrue(pending.completed().await(4, TimeUnit.SECONDS));
                pending.thread().join(2_000);
                assertFalse(pending.thread().isAlive());
                assertCourseFailure(pending.failure().get(), ErrorCode.REQUEST_TIMEOUT);
                assertEquals(2, cluster.partitionLog(1, tp).logEndOffset());
                assertEquals(1, cluster.tracker(tp).highWatermark());
            } finally {
                releaseProducer(cluster, tp, pending.thread());
            }
        }
    }

    private static Messages.Reply produce(ClusterHarness cluster, TopicPartition tp, int epoch,
                                          Acks acks, List<RecordData> records) {
        return produce(cluster, tp, epoch, acks, records, 5_000, 3_000);
    }

    private static Messages.Reply produce(ClusterHarness cluster, TopicPartition tp, int epoch,
                                          Acks acks, List<RecordData> records,
                                          long requestTimeoutMillis, int clientTimeoutMillis) {
        try (RpcClient client = new RpcClient(cluster.endpoint(1), clientTimeoutMillis)) {
            return client.call(new Messages.ProduceRequest(tp, epoch, acks, requestTimeoutMillis, records));
        }
    }

    private static AsyncProduce startProduce(ClusterHarness cluster, TopicPartition tp, Acks acks,
                                             List<RecordData> records, long requestTimeoutMillis,
                                             int clientTimeoutMillis) throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Messages.Reply> result = new AtomicReference<>();
        AtomicReference<Throwable> failure = new AtomicReference<>();
        Thread thread = new Thread(() -> {
            started.countDown();
            try {
                result.set(produce(cluster, tp, 0, acks, records, requestTimeoutMillis, clientTimeoutMillis));
            } catch (Throwable thrown) {
                failure.set(thrown);
            } finally {
                completed.countDown();
            }
        }, "step24-" + acks + "-producer");
        thread.setDaemon(true);
        thread.start();
        assertTrue(started.await(2, TimeUnit.SECONDS));
        return new AsyncProduce(thread, completed, result, failure);
    }

    private static Messages.Reply awaitSuccessfulProduce(AsyncProduce pending) throws InterruptedException {
        assertTrue(pending.completed().await(3, TimeUnit.SECONDS));
        pending.thread().join(2_000);
        assertFalse(pending.thread().isAlive());
        assertNull(pending.failure().get());
        Messages.Reply reply = pending.result().get();
        assertEquals(ErrorCode.NONE, reply.error());
        return reply;
    }

    private static void releaseProducer(ClusterHarness cluster, TopicPartition tp, Thread thread)
            throws InterruptedException {
        if (thread.isAlive()) {
            cluster.stopBroker(cluster.authority().metadata(tp).leaderId());
            thread.interrupt();
        }
        thread.join(3_000);
        assertFalse(thread.isAlive(), "producer worker did not terminate");
    }

    private static Messages.Reply fetchReply(ClusterHarness cluster, TopicPartition tp, long offset,
                                             int maxRecords, int maxBytes) {
        try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
            return client.call(new Messages.FetchRequest(tp, 0, offset, maxRecords, maxBytes, null));
        }
    }

    private static Messages.FetchBody fetch(ClusterHarness cluster, TopicPartition tp, long offset) {
        return fetch(cluster, tp, offset, 10, 65_536);
    }

    private static Messages.FetchBody fetch(ClusterHarness cluster, TopicPartition tp, long offset,
                                           int maxRecords, int maxBytes) {
        Messages.Reply reply = fetchReply(cluster, tp, offset, maxRecords, maxBytes);
        assertEquals(ErrorCode.NONE, reply.error());
        return (Messages.FetchBody) reply.body();
    }

    private static void assertUnchangedAfterReply(ClusterHarness cluster, TopicPartition tp,
                                                  ReplicationSnapshot before, List<Long> beforeLEOs,
                                                  ErrorCode expected, Messages.Reply reply) {
        assertEquals(expected, reply.error());
        assertUnchanged(cluster, tp, before, beforeLEOs);
    }

    private static void assertUnchanged(ClusterHarness cluster, TopicPartition tp,
                                        ReplicationSnapshot before, List<Long> beforeLEOs) {
        assertEquals(before, cluster.tracker(tp).snapshot());
        assertEquals(beforeLEOs, logEnds(cluster, tp));
    }

    private static List<Long> logEnds(ClusterHarness cluster, TopicPartition tp) {
        return List.of(cluster.partitionLog(1, tp).logEndOffset(),
                cluster.partitionLog(2, tp).logEndOffset(), cluster.partitionLog(3, tp).logEndOffset());
    }

    private static void awaitLeaderEnd(ClusterHarness cluster, TopicPartition tp, long expected, Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && cluster.partitionLog(1, tp).logEndOffset() < expected
                && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(expected, cluster.partitionLog(1, tp).logEndOffset(),
                "producer did not append before the test deadline");
    }

    private static List<Long> offsets(List<LogRecord> records) {
        return records.stream().map(LogRecord::offset).toList();
    }

    private static List<String> values(List<LogRecord> records) {
        return records.stream().map(record -> new String(record.data().value(), StandardCharsets.UTF_8)).toList();
    }

    private static void assertCourseFailure(Throwable failure, ErrorCode expected) {
        assertTrue(failure instanceof CourseException, "unexpected producer failure: " + failure);
        assertEquals(expected, ((CourseException) failure).code());
    }

    private record AsyncProduce(Thread thread, CountDownLatch completed, AtomicReference<Messages.Reply> result,
                                AtomicReference<Throwable> failure) {}

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
