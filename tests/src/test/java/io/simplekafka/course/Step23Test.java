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
import io.simplekafka.replication.AckPolicy;
import io.simplekafka.replication.ReplicationTracker;
import io.simplekafka.support.TimeSource;
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

class Step23Test {
    @TempDir Path root;

    @Test
    void allWaitsForCommittedProgressWhileLeaderAckIsImmediateAndTimeoutDoesNotUndoAppend() throws Exception {
        AtomicLong now = new AtomicLong(100);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step23", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            AckPolicy policy = new AckPolicy(tracker);
            AppendResult first = cluster.partitionLog(1, tp).append(List.of(record("first", 1)));
            tracker.report(1, 0, first.nextOffset());
            policy.validateBeforeAppend(Acks.ALL);
            policy.await(first, Acks.LEADER, 0, System.nanoTime() + TimeUnit.SECONDS.toNanos(2));
            assertEquals(0, tracker.highWatermark());

            CountDownLatch started = new CountDownLatch(1);
            CountDownLatch completed = new CountDownLatch(1);
            AtomicReference<Throwable> failure = new AtomicReference<>();
            Thread waiter = new Thread(() -> {
                started.countDown();
                try {
                    policy.await(first, Acks.ALL, 0, System.nanoTime() + TimeUnit.SECONDS.toNanos(5));
                } catch (Throwable thrown) {
                    failure.set(thrown);
                } finally {
                    completed.countDown();
                }
            }, "step23-all-ack-waiter");
            waiter.setDaemon(true);
            waiter.start();
            assertTrue(started.await(2, TimeUnit.SECONDS));
            awaitConditionWait(waiter);
            assertFalse(completed.await(0, TimeUnit.NANOSECONDS));

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                tracker.report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(1, tracker.advanceHighWatermark());
            assertTrue(completed.await(2, TimeUnit.SECONDS));
            waiter.join(2_000);
            assertFalse(waiter.isAlive());
            assertNull(failure.get());

            AppendResult timedOut = cluster.partitionLog(1, tp).append(List.of(record("uncommitted", 2)));
            tracker.report(1, 0, timedOut.nextOffset());
            CourseException timeout = org.junit.jupiter.api.Assertions.assertThrows(CourseException.class,
                    () -> policy.await(timedOut, Acks.ALL, 0, System.nanoTime() - 1));
            assertEquals(ErrorCode.REQUEST_TIMEOUT, timeout.code());
            assertEquals(2, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(1, tracker.highWatermark());

            now.set(1_100);
            assertEquals(Set.of(2, 3), tracker.expireLagging());
            CourseException interrupted = org.junit.jupiter.api.Assertions.assertThrows(CourseException.class,
                    () -> policy.await(timedOut, Acks.ALL, 0, System.nanoTime() + TimeUnit.SECONDS.toNanos(2)));
            assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, interrupted.code());
            CourseException insufficient = org.junit.jupiter.api.Assertions.assertThrows(CourseException.class,
                    () -> policy.validateBeforeAppend(Acks.ALL));
            assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, insufficient.code());
        }
    }

    private static void awaitConditionWait(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.isAlive() && thread.getState() != Thread.State.WAITING
                && thread.getState() != Thread.State.TIMED_WAITING && System.nanoTime() < deadline) {
            Thread.onSpinWait();
        }
        assertTrue(thread.getState() == Thread.State.WAITING || thread.getState() == Thread.State.TIMED_WAITING,
                "ALL acknowledgment did not wait for follower progress");
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
