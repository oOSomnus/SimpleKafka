package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
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
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step25Test {
    @TempDir Path root;

    @Test
    void electsTheLowestEligibleIsrAndFencesOldEpochWithoutLosingCommittedPrefix() {
        TimeSource clock = new AtomicLong(0)::get;
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

            cluster.stopBroker(1);
            var elected = cluster.elect(tp);
            assertEquals(2, elected.leaderId());
            assertEquals(1, elected.epoch());
            assertEquals(List.of(2), elected.isr());
            assertEquals(2, cluster.snapshot(tp).highWatermark());
            assertEquals(committedRecords(committed), cluster.partitionLog(2, tp).read(0, 10, 65_536));

            Messages.Reply staleProduce = produce(cluster, tp, 2, 0, Acks.LEADER, List.of(record("stale", 12)));
            assertEquals(ErrorCode.FENCED_EPOCH, staleProduce.error());
            assertEquals(2, cluster.partitionLog(2, tp).logEndOffset());
            assertCode(ErrorCode.FENCED_EPOCH, () -> new LeaderElection(cluster.authority()).checkLeader(2, tp, 0));
            assertCode(ErrorCode.NOT_LEADER, () -> new LeaderElection(cluster.authority()).checkLeader(3, tp, 1));

            cluster.stopBroker(2);
            ReplicationSnapshot beforeFailedElection = cluster.snapshot(tp);
            CourseException noLeader = assertThrows(CourseException.class, () -> cluster.elect(tp));
            assertEquals(ErrorCode.NO_ELIGIBLE_LEADER, noLeader.code());
            assertEquals(beforeFailedElection, cluster.snapshot(tp));
            assertEquals(1, cluster.authority().metadata(tp).epoch());
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
