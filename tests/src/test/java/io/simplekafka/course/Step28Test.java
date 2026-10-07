package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.client.GroupConsumer;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step28Test {
    @TempDir Path root;

    @Test
    void recoveredProofRestoresIsrForAllAcksAndAllReplicaFilesConverge() throws Exception {
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
                for (int brokerId : List.of(1, 3)) {
                    assertEquals(leaderRecords, cluster.partitionLog(brokerId, tp).read(0, 10, 65_536));
                }
                List<List<byte[]>> diskImages = partitionDiskImages(root, tp.topic());
                assertEquals(3, diskImages.size());
                for (int broker = 1; broker < diskImages.size(); broker++) {
                    assertEquals(diskImages.get(0).size(), diskImages.get(broker).size());
                    for (int file = 0; file < diskImages.get(0).size(); file++)
                        assertArrayEquals(diskImages.get(0).get(file), diskImages.get(broker).get(file));
                }
            }
        }
    }

    @Test
    void admissionRejectsAProofMadeStaleByALeaderAppendAndRejectsUnassignedOrOfflineBrokers() {
        TopicPartition tp = new TopicPartition("step28proof", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(new AppendResult(0, 1), produce(cluster, tp, 1, 0, Acks.LEADER,
                    List.of(record("committed", 30))).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, 1);
            }
            assertEquals(1, cluster.tracker(tp).highWatermark());

            cluster.stopBroker(1);
            cluster.elect(tp);
            cluster.restartBroker(1);
            assertEquals(1, cluster.reconcile(1, tp));
            assertEquals(new AppendResult(1, 2), produce(cluster, tp, 2, 1, Acks.LEADER,
                    List.of(record("leader-advanced", 31))).result());
            assertFalse(cluster.admit(1, tp), "proof for the previous leader LEO must not admit a stale copy");
            assertEquals(List.of(2), cluster.snapshot(tp).isr());

            assertEquals(2, cluster.reconcile(1, tp));
            assertTrue(cluster.admit(1, tp));
            assertEquals(2, cluster.snapshot(tp).highWatermark());
            assertEquals(List.of(1, 2), cluster.snapshot(tp).isr());
            CourseException unassigned = org.junit.jupiter.api.Assertions.assertThrows(CourseException.class,
                    () -> cluster.admit(4, tp));
            assertEquals(ErrorCode.INVALID_REQUEST, unassigned.code());

            ReplicationSnapshot beforeOfflineAttempt = cluster.snapshot(tp);
            cluster.stopBroker(1);
            assertFalse(cluster.admit(1, tp));
            assertEquals(beforeOfflineAttempt, cluster.snapshot(tp));
        }
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


    private static List<List<byte[]>> partitionDiskImages(Path root, String topic) throws IOException {
        List<List<byte[]>> brokerImages = new java.util.ArrayList<>(3);
        for (int brokerId : List.of(1, 2, 3)) {
            Path directory = root.resolve("broker-" + brokerId).resolve(topic).resolve("0");
            try (Stream<Path> paths = Files.list(directory)) {
                brokerImages.add(paths.filter(Files::isRegularFile).sorted().map(path -> {
                    try {
                        return Files.readAllBytes(path);
                    } catch (IOException exception) {
                        throw new java.io.UncheckedIOException(exception);
                    }
                }).toList());
            }
        }
        return List.copyOf(brokerImages);
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
