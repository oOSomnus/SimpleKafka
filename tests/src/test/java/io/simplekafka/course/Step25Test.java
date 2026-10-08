package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.replication.LeaderElection;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

class Step25Test {
    @TempDir Path root;

    @Test
    @DisplayName("Elects the lowest eligible ISR and fences old epoch without losing committed prefix")
    void electsTheLowestEligibleIsrAndFencesOldEpochWithoutLosingCommittedPrefix() {
        AtomicLong now = new AtomicLong(0);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step25", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = List.of(record("zero", 10), record("one", 11));
            Messages.Reply initial = produce(cluster, tp, 1, 0, Acks.LEADER, committed);
            assertEquals(new AppendResult(0, 2), ((Messages.ProduceBody) initial.body()).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(2, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 2);
            }
            assertEquals(2, cluster.tracker(tp).highWatermark());
            assertEquals(Map.of(1, 2L, 2, 2L, 3, 2L), cluster.snapshot(tp).perReplicaLEO());

            ReplicationSnapshot beforeOnlineElections = cluster.snapshot(tp);
            Map<Integer, Long> caughtUpBeforeOnlineElections = caughtUpTimes(cluster.authority(), tp);
            var metadataBeforeOnlineElections = cluster.authority().metadata(tp);
            now.set(10);
            var firstOnlineElection = cluster.elect(tp);
            var secondOnlineElection = cluster.elect(tp);
            assertEquals(1, firstOnlineElection.leaderId());
            assertEquals(0, firstOnlineElection.epoch());
            assertEquals(metadataBeforeOnlineElections, firstOnlineElection);
            assertEquals(firstOnlineElection, secondOnlineElection);
            assertEquals(beforeOnlineElections, cluster.snapshot(tp));
            assertEquals(caughtUpBeforeOnlineElections, caughtUpTimes(cluster.authority(), tp));

            assertCode(ErrorCode.NOT_LEADER,
                    () -> new LeaderElection(cluster.authority()).checkLeader(2, tp, 0));
            cluster.stopBroker(1);
            cluster.stopBroker(2);
            var elected = cluster.elect(tp);
            assertEquals(3, elected.leaderId());
            assertEquals(1, elected.epoch());
            assertEquals(List.of(3), elected.isr());
            assertEquals(2, cluster.snapshot(tp).highWatermark());
            assertEquals(Map.of(1, 0L, 2, 0L, 3, 2L), cluster.snapshot(tp).perReplicaLEO());
            assertEquals(committedRecords(committed), cluster.partitionLog(3, tp).read(0, 10, 65_536));

            Messages.Reply staleProduce = produce(cluster, tp, 3, 0, Acks.LEADER, List.of(record("stale", 12)));
            assertEquals(ErrorCode.FENCED_EPOCH, staleProduce.error());
            assertEquals(2, cluster.partitionLog(3, tp).logEndOffset());
            assertCode(ErrorCode.FENCED_EPOCH, () -> new LeaderElection(cluster.authority()).checkLeader(3, tp, 0));

            cluster.restartBroker(1);
            assertTrue(cluster.authority().isOnline(1));
            assertCode(ErrorCode.NOT_LEADER,
                    () -> new LeaderElection(cluster.authority()).checkLeader(1, tp, 1));
            ReplicationSnapshot beforeFailedElection = cluster.snapshot(tp);
            Map<Integer, Long> caughtUpBeforeFailedElection = caughtUpTimes(cluster.authority(), tp);
            var metadataBeforeFailedElection = cluster.authority().metadata(tp);
            assertEquals(List.of(3), beforeFailedElection.isr());
            assertFalse(beforeFailedElection.isr().contains(1));
            cluster.stopBroker(3);
            CourseException noLeader = assertThrows(CourseException.class, () -> cluster.elect(tp));
            assertEquals(ErrorCode.NO_ELIGIBLE_LEADER, noLeader.code());
            assertEquals(beforeFailedElection, cluster.snapshot(tp));
            assertEquals(caughtUpBeforeFailedElection, caughtUpTimes(cluster.authority(), tp));
            assertEquals(metadataBeforeFailedElection, cluster.authority().metadata(tp));
            assertFalse(cluster.authority().isOnline(3));
        }
    }


    @Test
    @DisplayName("Election fences an outstanding all ack without rolling back append")
    void electionFencesAnOutstandingAllAckWithoutRollingBackAppend() throws Exception {
        TopicPartition tp = new TopicPartition("step25-pending-all", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Messages.Reply> reply = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                started.countDown();
                try (RpcClient client = new RpcClient(cluster.endpoint(1), 10_000)) {
                    reply.set(client.call(new Messages.ProduceRequest(
                            tp, 0, Acks.ALL, 5_000, List.of(record("pending", 10)))));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    completed.countDown();
                }
            }, "step25-pending-all-producer");
            producer.setDaemon(true);
            producer.start();

            try {
                assertTrue(started.await(2, TimeUnit.SECONDS));
                long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                while (producer.isAlive() && cluster.partitionLog(1, tp).logEndOffset() < 1
                        && System.nanoTime() < deadline) Thread.onSpinWait();
                assertEquals(1, cluster.partitionLog(1, tp).logEndOffset(),
                        "the append must reach disk before election");
                assertEquals(0, cluster.tracker(tp).highWatermark());
                ClusterAuthority.PartitionState state = cluster.authority().partitionState(tp);
                awaitConditionWaiter(state, producer);

                state.lock.lock();
                try {
                    cluster.authority().setBrokerOnline(1, false);
                    var elected = cluster.elect(tp);
                    assertEquals(2, elected.leaderId());
                    assertEquals(1, elected.epoch());
                    assertEquals(List.of(2), elected.isr());
                    assertEquals(0, cluster.tracker(tp).highWatermark());
                } finally {
                    state.lock.unlock();
                }

                assertTrue(completed.await(3, TimeUnit.SECONDS));
                producer.join(2_000);
                assertFalse(producer.isAlive());
                assertNull(failure.get());
                assertEquals(ErrorCode.FENCED_EPOCH, reply.get().error());
                assertEquals(List.of(new LogRecord(0, record("pending", 10))),
                        cluster.partitionLog(1, tp).read(0, 10, 4_096));
                assertEquals(List.of(), cluster.partitionLog(2, tp).read(0, 10, 4_096));
            } finally {
                if (producer.isAlive()) {
                    producer.interrupt();
                    producer.join(3_000);
                }
                assertFalse(producer.isAlive(), "the outstanding ALL producer must terminate");
            }
        }
    }

    private static void awaitConditionWaiter(ClusterAuthority.PartitionState state, Thread producer) {
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
        assertTrue(waiting, "the actual ALL request must be waiting for high-watermark progress");
    }

    @Test
    @DisplayName("Election requires the committed prefix before choosing the lowest ISR")
    void electionRequiresTheCommittedPrefixBeforeChoosingTheLowestIsr() {
        TimeSource clock = new AtomicLong(0)::get;
        ClusterAuthority authority = new ClusterAuthority(clock);
        for (int brokerId : List.of(1, 2, 3))
            authority.registerBroker(brokerId, new Endpoint("127.0.0.1", 12_000 + brokerId));
        TopicPartition tp = new TopicPartition("step25-qualification", 0);
        ClusterAuthority.PartitionState state = authority.createPartition(tp, 1, List.of(1, 2, 3), 1);
        state.lock.lock();
        try {
            state.reportedLEO.put(1, 3L);
            state.reportedLEO.put(2, 1L);
            state.reportedLEO.put(3, 3L);
            state.highWatermark = 3;
        } finally {
            state.lock.unlock();
        }
        authority.setBrokerOnline(1, false);
        assertFalse(authority.isOnline(1));
        assertEquals(Map.of(1, 3L, 2, 1L, 3, 3L), authority.snapshot(tp).perReplicaLEO());
        assertEquals(3, authority.snapshot(tp).highWatermark());


        var elected = new LeaderElection(authority).elect(tp);
        assertEquals(3, elected.leaderId());
        assertEquals(1, elected.epoch());
        assertEquals(List.of(3), elected.isr());
        assertEquals(3, authority.snapshot(tp).highWatermark());
    }

    private static Map<Integer, Long> caughtUpTimes(ClusterAuthority authority, TopicPartition tp) {
        ClusterAuthority.PartitionState state = authority.partitionState(tp);
        state.lock.lock();
        try {
            return Map.copyOf(state.lastCaughtUpMillis);
        } finally {
            state.lock.unlock();
        }
    }

    private static Messages.Reply produce(ClusterHarness cluster, TopicPartition tp, int brokerId, int epoch,
                                          Acks acks, List<RecordData> records) {
        try (RpcClient client = new RpcClient(cluster.endpoint(brokerId), 3_000)) {
            return client.call(new Messages.ProduceRequest(tp, epoch, acks, 2_000, records));
        }
    }

    private static List<LogRecord> committedRecords(List<RecordData> data) {
        return List.of(new LogRecord(0, data.get(0)), new LogRecord(1, data.get(1)));
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }

    private static void assertCode(ErrorCode expected, org.junit.jupiter.api.function.Executable action) {
        CourseException exception = assertThrows(CourseException.class, action);
        assertEquals(expected, exception.code());
    }
}
