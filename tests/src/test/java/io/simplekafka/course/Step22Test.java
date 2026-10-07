package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.replication.ReplicationTracker;
import io.simplekafka.support.TimeSource;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step22Test {
    @TempDir Path root;

    @Test
    void reportsAdvanceOnlyTheCommittedPrefixAndLagExpiryFreezesItBelowMinIsr() {
        AtomicLong now = new AtomicLong(100);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step22", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            ReplicationTracker tracker = cluster.tracker(tp);
            List<RecordData> five = new ArrayList<>();
            for (int i = 0; i < 5; i++) five.add(record("entry-" + i, 100 + i));
            cluster.partitionLog(1, tp).append(five);

            assertEquals(3, cluster.replicator(2, tp).pollOnce(3, 65_536));
            assertEquals(4, cluster.replicator(3, tp).pollOnce(4, 65_536));
            tracker.report(1, 0, 5);
            tracker.report(2, 0, 3);
            tracker.report(3, 0, 4);
            assertEquals(3, tracker.advanceHighWatermark());

            ReplicationSnapshot beforeInvalidReports = tracker.snapshot();
            assertCode(ErrorCode.INVALID_REQUEST, () -> tracker.report(4, 0, 5));
            assertCode(ErrorCode.INVALID_REQUEST, () -> tracker.report(2, 0, 6));
            assertCode(ErrorCode.INVALID_REQUEST, () -> tracker.report(2, 0, 2));
            assertCode(ErrorCode.FENCED_EPOCH, () -> tracker.report(2, 1, 3));
            assertEquals(beforeInvalidReports, tracker.snapshot());

            assertEquals(2, cluster.replicator(2, tp).pollOnce(5, 65_536));
            tracker.report(2, 0, 5);
            assertEquals(4, tracker.advanceHighWatermark());
            assertEquals(1, cluster.replicator(3, tp).pollOnce(5, 65_536));
            tracker.report(3, 0, 5);
            assertEquals(5, tracker.advanceHighWatermark());

            now.set(1_099);
            assertEquals(Set.of(), tracker.expireLagging());
            now.set(1_100);
            assertEquals(Set.of(2, 3), tracker.expireLagging());
            assertEquals(List.of(1), tracker.snapshot().isr());

            cluster.partitionLog(1, tp).append(List.of(record("uncommitted", 106)));
            tracker.report(1, 0, 6);
            assertEquals(5, tracker.advanceHighWatermark());
            assertEquals(5, tracker.highWatermark());
        }
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }

    private static void assertCode(ErrorCode expected, org.junit.jupiter.api.function.Executable action) {
        CourseException exception = assertThrows(CourseException.class, action);
        assertEquals(expected, exception.code());
    }
}
