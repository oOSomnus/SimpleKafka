package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.io.TempDir;

class Step23Test {
    @TempDir Path root;

    @Test
    @DisplayName("Insufficient ISR rejects all before append while leader ack still appends")
    void insufficientIsrRejectsAllBeforeAppendWhileLeaderAckStillAppends() {
        AtomicLong now = new AtomicLong(1_000);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step23-precheck", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            AckPolicy policy = new AckPolicy(tracker);

            now.set(2_000);
            assertEquals(Set.of(2, 3), tracker.expireLagging());
            ReplicationSnapshot beforeAll = tracker.snapshot();
            CourseException insufficient = assertThrows(CourseException.class,
                    () -> policy.validateBeforeAppend(Acks.ALL));
            assertEquals(ErrorCode.NOT_ENOUGH_REPLICAS, insufficient.code());
            assertEquals(0, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(beforeAll, tracker.snapshot());

            AppendResult leaderAcked = appendAsLeader(cluster, tp, 0, Acks.LEADER,
                    List.of(record("leader-only", 2)));
            assertEquals(new AppendResult(0, 1), leaderAcked);
            assertEquals(1, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(0, tracker.highWatermark());
        }
    }

    @Test
    @DisplayName("Exact high watermark boundary succeeds but one offset below waits for progress")
    void exactHighWatermarkBoundarySucceedsButOneOffsetBelowWaitsForProgress() throws Exception {
        TimeSource clock = new AtomicLong(1_000)::get;
        TopicPartition tp = new TopicPartition("step23-boundary", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            AckPolicy policy = new AckPolicy(tracker);

            AppendResult first = appendAsLeader(cluster, tp, 0, Acks.LEADER, List.of(record("first", 1)));
            tracker.report(2, 0, 1);
            tracker.report(3, 0, 1);
            assertEquals(1, tracker.highWatermark());
            policy.await(first, Acks.ALL, 0, System.nanoTime() - TimeUnit.MILLISECONDS.toNanos(1));

            AppendResult second = appendAsLeader(cluster, tp, 0, Acks.LEADER, List.of(record("second", 2)));
            assertEquals(new AppendResult(1, 2), second);
            assertEquals(1, tracker.highWatermark());
            AckWaiter waiter = startWaiter(policy, second, 0, 5);
            try {
                awaitConditionWait(waiter.thread());
                assertFalse(waiter.completed().await(200, TimeUnit.MILLISECONDS));
                assertEquals(1, tracker.highWatermark());

                tracker.report(2, 0, 2);
                assertEquals(1, tracker.highWatermark());
                tracker.report(3, 0, 2);
                assertEquals(2, tracker.highWatermark());
                assertWaiterSucceeded(waiter);
            } finally {
                stopWaiter(waiter);
            }
        }
    }

    @Test
    @DisplayName("Zero timeout retains the append without advancing the high watermark")
    void zeroTimeoutRetainsTheAppendWithoutAdvancingTheHighWatermark() {
        TimeSource clock = new AtomicLong(1_000)::get;
        TopicPartition tp = new TopicPartition("step23-zero-timeout", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            AckPolicy policy = new AckPolicy(tracker);

            policy.validateBeforeAppend(Acks.ALL);
            AppendResult pending = cluster.partitionLog(1, tp).append(List.of(record("retained", 1)));
            tracker.report(1, 0, pending.nextOffset());
            CourseException timeout = assertThrows(CourseException.class,
                    () -> policy.await(pending, Acks.ALL, 0, System.nanoTime()));
            assertEquals(ErrorCode.REQUEST_TIMEOUT, timeout.code());
            assertEquals(1, cluster.partitionLog(1, tp).logEndOffset());
            assertEquals(0, tracker.highWatermark());
            assertEquals(1L, tracker.snapshot().perReplicaLEO().get(1));
        }
    }

    @Test
    @DisplayName("Epoch change while waiting fences the acknowledgement")
    void epochChangeWhileWaitingFencesTheAcknowledgement() throws Exception {
        TimeSource clock = new AtomicLong(1_000)::get;
        TopicPartition tp = new TopicPartition("step23-epoch", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            AckPolicy policy = new AckPolicy(tracker);
            AppendResult pending = appendAsLeader(cluster, tp, 0, Acks.LEADER, List.of(record("pending", 1)));
            AckWaiter waiter = startWaiter(policy, pending, 0, 5);
            try {
                awaitConditionWait(waiter.thread());
                advanceEpoch(cluster.authority(), tp);
                assertTrue(waiter.completed().await(2, TimeUnit.SECONDS));
                waiter.thread().join(2_000);
                assertFalse(waiter.thread().isAlive());
                assertCourseFailure(waiter.failure().get(), ErrorCode.FENCED_EPOCH);
                assertEquals(1, cluster.partitionLog(1, tp).logEndOffset());
                assertEquals(0, tracker.highWatermark());
            } finally {
                stopWaiter(waiter);
            }
        }
    }

    @Test
    @DisplayName("ISR reduction while waiting fails and interruption preserves the interrupt flag")
    void isrReductionWhileWaitingFailsAndInterruptionPreservesTheInterruptFlag() throws Exception {
        AtomicLong now = new AtomicLong(1_000);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step23-isr", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            AckPolicy policy = new AckPolicy(tracker);
            AppendResult pending = appendAsLeader(cluster, tp, 0, Acks.LEADER, List.of(record("pending", 1)));
            AckWaiter waiter = startWaiter(policy, pending, 0, 5);
            try {
                awaitConditionWait(waiter.thread());
                now.set(2_000);
                assertEquals(Set.of(2, 3), tracker.expireLagging());
                assertTrue(waiter.completed().await(2, TimeUnit.SECONDS));
                waiter.thread().join(2_000);
                assertFalse(waiter.thread().isAlive());
                assertCourseFailure(waiter.failure().get(), ErrorCode.NOT_ENOUGH_REPLICAS);
                assertEquals(1, cluster.partitionLog(1, tp).logEndOffset());
                assertEquals(0, tracker.highWatermark());
            } finally {
                stopWaiter(waiter);
            }
        }

        now.set(1_000);
        TopicPartition interruptedTp = new TopicPartition("step23-interrupted", 0);
        try (ClusterHarness cluster = new ClusterHarness(root.resolve("interrupted"), clock)) {
            cluster.start();
            cluster.createTopic(interruptedTp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(interruptedTp);
            AckPolicy policy = new AckPolicy(tracker);
            AppendResult pending = appendAsLeader(cluster, interruptedTp, 0, Acks.LEADER,
                    List.of(record("pending", 1)));
            AckWaiter waiter = startWaiter(policy, pending, 0, 5);
            try {
                awaitConditionWait(waiter.thread());
                waiter.thread().interrupt();
                assertTrue(waiter.completed().await(2, TimeUnit.SECONDS));
                waiter.thread().join(2_000);
                assertFalse(waiter.thread().isAlive());
                assertCourseFailure(waiter.failure().get(), ErrorCode.REQUEST_TIMEOUT);
                assertTrue(waiter.interruptPreserved().get());
                assertEquals(1, cluster.partitionLog(1, interruptedTp).logEndOffset());
                assertEquals(0, tracker.highWatermark());
            } finally {
                stopWaiter(waiter);
            }
        }
    }

    private static AppendResult appendAsLeader(ClusterHarness cluster, TopicPartition tp, int epoch,
                                                Acks acks, List<RecordData> records) {
        ReplicationTracker tracker = cluster.tracker(tp);
        AckPolicy policy = new AckPolicy(tracker);
        policy.validateBeforeAppend(acks);
        AppendResult result = cluster.partitionLog(1, tp).append(records);
        tracker.report(1, epoch, result.nextOffset());
        policy.await(result, acks, epoch, System.nanoTime() + TimeUnit.SECONDS.toNanos(1));
        return result;
    }

    private static void advanceEpoch(io.simplekafka.cluster.ClusterAuthority authority, TopicPartition tp) {
        var state = authority.partitionState(tp);
        state.lock.lock();
        try {
            state.epoch++;
            state.changed.signalAll();
        } finally {
            state.lock.unlock();
        }
    }

    private static AckWaiter startWaiter(AckPolicy policy, AppendResult result, int epoch, long timeoutSeconds)
            throws InterruptedException {
        CountDownLatch started = new CountDownLatch(1);
        CountDownLatch completed = new CountDownLatch(1);
        AtomicReference<Throwable> failure = new AtomicReference<>();
        AtomicBoolean interruptPreserved = new AtomicBoolean();
        Thread thread = new Thread(() -> {
            started.countDown();
            try {
                policy.await(result, Acks.ALL, epoch,
                        System.nanoTime() + TimeUnit.SECONDS.toNanos(timeoutSeconds));
            } catch (Throwable thrown) {
                failure.set(thrown);
            } finally {
                interruptPreserved.set(Thread.currentThread().isInterrupted());
                completed.countDown();
            }
        }, "step23-all-ack-waiter");
        thread.setDaemon(true);
        thread.start();
        assertTrue(started.await(2, TimeUnit.SECONDS));
        return new AckWaiter(thread, completed, failure, interruptPreserved);
    }

    private static void awaitConditionWait(Thread thread) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(2);
        while (thread.isAlive() && !isWaiting(thread.getState()) && System.nanoTime() < deadline)
            Thread.onSpinWait();
        assertTrue(isWaiting(thread.getState()), "ALL acknowledgment did not wait for follower progress");
    }

    private static boolean isWaiting(Thread.State state) {
        return state == Thread.State.WAITING || state == Thread.State.TIMED_WAITING;
    }

    private static void assertWaiterSucceeded(AckWaiter waiter) throws InterruptedException {
        assertTrue(waiter.completed().await(2, TimeUnit.SECONDS));
        waiter.thread().join(2_000);
        assertFalse(waiter.thread().isAlive());
        assertNull(waiter.failure().get());
    }

    private static void assertCourseFailure(Throwable failure, ErrorCode expected) {
        assertTrue(failure instanceof CourseException, "unexpected waiter failure: " + failure);
        assertEquals(expected, ((CourseException) failure).code());
    }

    private static void stopWaiter(AckWaiter waiter) throws InterruptedException {
        if (waiter.thread().isAlive()) waiter.thread().interrupt();
        waiter.thread().join(2_000);
        assertFalse(waiter.thread().isAlive(), "acknowledgement worker did not terminate");
    }

    private record AckWaiter(Thread thread, CountDownLatch completed, AtomicReference<Throwable> failure,
                             AtomicBoolean interruptPreserved) {}

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
