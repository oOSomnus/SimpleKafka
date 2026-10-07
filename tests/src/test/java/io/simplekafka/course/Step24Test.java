package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step24Test {
    @TempDir Path root;

    @Test
    void allAckWaitDoesNotBlockCommittedFetchAndCompletesAfterRealTcpReplication() throws Exception {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step24", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Messages.ProduceBody prefix = (Messages.ProduceBody) produce(cluster, tp, 0,
                    Acks.LEADER, List.of(record("committed-prefix", 1))).body();
            assertEquals(new AppendResult(0, 1), prefix.result());

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(1, cluster.tracker(tp).highWatermark());
            assertEquals(List.of("committed-prefix"), values(fetch(cluster, tp, 0).records()));
            assertEquals(List.of(), fetch(cluster, tp, 1).records());
            assertEquals(ErrorCode.OFFSET_OUT_OF_RANGE, fetchReply(cluster, tp, 3).error());

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Messages.Reply> result = new AtomicReference<>();
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                started.countDown();
                try {
                    result.set(produce(cluster, tp, 0, Acks.ALL, List.of(record("newly-replicated", 2))));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    completed.countDown();
                }
            }, "step24-all-produce");
            producer.setDaemon(true);
            producer.start();
            assertTrue(started.await(2, TimeUnit.SECONDS));
            awaitLeaderEnd(cluster, tp, 2, producer);
            assertFalse(completed.await(0, TimeUnit.NANOSECONDS));

            Messages.FetchBody duringWait = fetch(cluster, tp, 0);
            assertEquals(1, duringWait.highWatermark());
            assertEquals(List.of("committed-prefix"), values(duringWait.records()));
            assertEquals(List.of(), fetch(cluster, tp, 1).records());

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(2, cluster.tracker(tp).highWatermark());
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            producer.join(2_000);
            assertFalse(producer.isAlive());
            assertNull(failure.get());
            assertEquals(ErrorCode.NONE, result.get().error());
            assertEquals(new AppendResult(1, 2), ((Messages.ProduceBody) result.get().body()).result());
            assertEquals(List.of("newly-replicated"), values(fetch(cluster, tp, 1).records()));
        }
    }

    @Test
    void stoppingLeaderClosesAnOutstandingAllAckRequest() throws Exception {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step24shutdown", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(new AppendResult(0, 1), ((Messages.ProduceBody) produce(cluster, tp, 0,
                    Acks.LEADER, List.of(record("visible", 1))).body()).result());
            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(1, cluster.tracker(tp).highWatermark());

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread producer = new Thread(() -> {
                started.countDown();
                try {
                    produce(cluster, tp, 0, Acks.ALL, List.of(record("pending", 2)));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    completed.countDown();
                }
            }, "step24-shutdown-pending-produce");
            producer.setDaemon(true);
            producer.start();
            assertTrue(started.await(2, TimeUnit.SECONDS));
            awaitLeaderEnd(cluster, tp, 2, producer);
            assertFalse(completed.await(0, TimeUnit.NANOSECONDS));

            cluster.stopBroker(1);
            assertTrue(completed.await(4, TimeUnit.SECONDS));
            producer.join(2_000);
            assertFalse(producer.isAlive());
            assertTrue(failure.get() instanceof CourseException);
            assertEquals(ErrorCode.REQUEST_TIMEOUT, ((CourseException) failure.get()).code());
            assertEquals(2, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(1, cluster.tracker(tp).highWatermark());
        }
    }

    private static Messages.Reply produce(ClusterHarness cluster, TopicPartition tp, int epoch,
                                          Acks acks, List<RecordData> records) {
        try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
            return client.call(new Messages.ProduceRequest(tp, epoch, acks, 5_000, records));
        }
    }

    private static Messages.Reply fetchReply(ClusterHarness cluster, TopicPartition tp, long offset) {
        try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
            return client.call(new Messages.FetchRequest(tp, 0, offset, 10, 65_536, null));
        }
    }

    private static Messages.FetchBody fetch(ClusterHarness cluster, TopicPartition tp, long offset) {
        Messages.Reply reply = fetchReply(cluster, tp, offset);
        assertEquals(ErrorCode.NONE, reply.error());
        return (Messages.FetchBody) reply.body();
    }

    private static void awaitLeaderEnd(ClusterHarness cluster, TopicPartition tp, long expected, Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
        while (thread.isAlive() && cluster.partitionLog(1, tp).logEndOffset() < expected
                && System.nanoTime() < deadline) Thread.onSpinWait();
        assertEquals(expected, cluster.partitionLog(1, tp).logEndOffset(), "producer did not append before the test deadline");
    }

    private static List<String> values(List<io.simplekafka.model.LogRecord> records) {
        return records.stream().map(record -> new String(record.data().value(), StandardCharsets.UTF_8)).toList();
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
