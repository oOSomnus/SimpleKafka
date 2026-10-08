package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.replication.ReplicationTracker;
import io.simplekafka.support.TimeSource;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

class Step22Test {
    @TempDir Path root;

    @Test
    @DisplayName(
            "Rejects invalid progress and expires only replicas that miss the exact catch up deadline")
    void rejectsInvalidProgressAndExpiresOnlyReplicasThatMissTheExactCatchUpDeadline() {
        AtomicLong now = new AtomicLong(1_000);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step22", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            cluster.authority().registerBroker(4, new Endpoint("127.0.0.1", 0));

            assertRejectedUnchanged(
                    tracker, ErrorCode.INVALID_REQUEST, () -> tracker.report(2, 0, -1));
            assertRejectedUnchanged(tracker, ErrorCode.FENCED_EPOCH, () -> tracker.report(2, 1, 0));
            assertRejectedUnchanged(
                    tracker, ErrorCode.INVALID_REQUEST, () -> tracker.report(99, 0, 0));
            assertRejectedUnchanged(
                    tracker, ErrorCode.INVALID_REQUEST, () -> tracker.report(4, 0, 0));
            cluster.authority().setBrokerOnline(3, false);
            assertRejectedUnchanged(
                    tracker, ErrorCode.INVALID_REQUEST, () -> tracker.report(3, 0, 0));
            cluster.authority().setBrokerOnline(3, true);

            tracker.report(1, 0, 3);
            tracker.report(2, 0, 3);
            assertRejectedUnchanged(
                    tracker, ErrorCode.INVALID_REQUEST, () -> tracker.report(3, 0, 4));
            assertRejectedUnchanged(
                    tracker, ErrorCode.INVALID_REQUEST, () -> tracker.report(2, 0, 2));

            now.set(1_500);
            tracker.report(2, 0, 3);
            tracker.report(3, 0, 2);
            assertEquals(2, tracker.highWatermark());

            now.set(1_999);
            ReplicationSnapshot beforeDeadline = tracker.snapshot();
            assertEquals(Set.of(), tracker.expireLagging());
            assertEquals(beforeDeadline, tracker.snapshot());
            assertEquals(2, tracker.highWatermark());

            now.set(2_000);
            assertEquals(Set.of(3), tracker.expireLagging());
            assertEquals(
                    new ReplicationSnapshot(
                            1, 0, List.of(1, 2, 3), List.of(1, 2), 3, Map.of(1, 3L, 2, 3L, 3, 2L)),
                    tracker.snapshot());
            assertEquals(3, tracker.highWatermark());

            tracker.report(3, 0, 3);
            assertEquals(List.of(1, 2), tracker.snapshot().isr());
            assertEquals(3, tracker.highWatermark());

            tracker.report(1, 0, 4);
            tracker.report(2, 0, 3);
            assertEquals(3, tracker.advanceHighWatermark());
            assertEquals(3, tracker.highWatermark());
            tracker.report(2, 0, 4);
            assertEquals(4, tracker.advanceHighWatermark());
            assertEquals(4, tracker.advanceHighWatermark());
            assertEquals(4, tracker.highWatermark());

            now.set(3_000);
            assertEquals(Set.of(2), tracker.expireLagging());
            assertEquals(List.of(1), tracker.snapshot().isr());
            tracker.report(1, 0, 5);
            assertEquals(4, tracker.advanceHighWatermark());
            assertEquals(Set.of(), tracker.expireLagging());
            assertEquals(
                    new ReplicationSnapshot(
                            1, 0, List.of(1, 2, 3), List.of(1), 4, Map.of(1, 5L, 2, 4L, 3, 3L)),
                    tracker.snapshot());
        }
    }

    private static void assertRejectedUnchanged(
            ReplicationTracker tracker,
            ErrorCode expected,
            org.junit.jupiter.api.function.Executable action) {
        ReplicationSnapshot before = tracker.snapshot();
        long highWatermark = tracker.highWatermark();
        assertCode(expected, action);
        assertEquals(before, tracker.snapshot());
        assertEquals(highWatermark, tracker.highWatermark());
    }

    private static void assertCode(
            ErrorCode expected, org.junit.jupiter.api.function.Executable action) {
        CourseException exception = assertThrows(CourseException.class, action);
        assertEquals(expected, exception.code());
    }
}
