package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.RecoveryProof;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.replication.ReplicaReconciler;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step26Test {
    @TempDir Path root;

    @Test
    void repairsOnlyDivergentUncommittedTailAndReturnsLeaderPrefixProof() {
        TopicPartition tp = new TopicPartition("step26", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = prefix();
            assertEquals(3, checkpoint(cluster, tp, committed));
            assertEquals(new AppendResult(3, 4), ((Messages.ProduceBody) produce(cluster, tp, 1, 0,
                    Acks.LEADER, List.of(record("old-tail", 30))).body()).result());

            cluster.stopBroker(1);
            var elected = cluster.elect(tp);
            assertEquals(2, elected.leaderId());
            cluster.restartBroker(1);
            assertEquals(new AppendResult(3, 4), ((Messages.ProduceBody) produce(cluster, tp, 2, 1,
                    Acks.LEADER, List.of(record("new-tail", 31))).body()).result());
            List<LogRecord> leaderPrefix = cluster.partitionLog(2, tp).read(0, 10, 65_536);
            ReplicationSnapshot beforeRepair = cluster.snapshot(tp);

            ReplicaReconciler reconciler = cluster.reconciler(1, tp);
            assertEquals(4, reconciler.reconcile(1));
            assertEquals(leaderPrefix, cluster.partitionLog(1, tp).read(0, 10, 65_536));
            assertEquals(beforeRepair, cluster.snapshot(tp), "recovery reads must not report progress or change ISR/HW");
            RecoveryProof proof = reconciler.recoveryProof().orElseThrow();
            assertEquals(1, proof.brokerId());
            assertEquals(tp, proof.tp());
            assertEquals(1, proof.epoch());
            assertEquals(4, proof.leaderLEO());
            assertEquals(4, proof.localLEO());
            assertEquals(3, proof.highWatermark());
        }
    }

    @Test
    void refusesAConflictInsideTheCommittedPrefixWithoutChangingTheDamagedReplica() {
        TopicPartition tp = new TopicPartition("step26corrupt", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            assertEquals(3, checkpoint(cluster, tp, prefix()));
            cluster.stopBroker(1);
            cluster.elect(tp);
            cluster.restartBroker(1);

            cluster.partitionLog(1, tp).truncateTo(0);
            cluster.partitionLog(1, tp).append(List.of(record("conflicting-committed-zero", 99)));
            List<LogRecord> damagedBefore = cluster.partitionLog(1, tp).read(0, 10, 65_536);
            ReplicationSnapshot before = cluster.snapshot(tp);
            CourseException corruption = assertThrows(CourseException.class, () -> cluster.reconcile(1, tp));
            assertEquals(ErrorCode.CORRUPT_RECORD, corruption.code());
            assertEquals(damagedBefore, cluster.partitionLog(1, tp).read(0, 10, 65_536));
            assertEquals(before, cluster.snapshot(tp));
        }
    }

    @Test
    void fillsAnIntactReplicaThatIsShorterThanTheCommittedPrefix() {
        TopicPartition tp = new TopicPartition("step26lagging", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> committed = prefix();
            assertEquals(3, checkpoint(cluster, tp, committed));
            cluster.stopBroker(1);
            cluster.elect(tp);
            cluster.restartBroker(1);
            cluster.partitionLog(1, tp).truncateTo(1);
            assertEquals(1, cluster.partitionLog(1, tp).logEndOffset());

            assertEquals(3, cluster.reconcile(1, tp));
            assertEquals(cluster.partitionLog(2, tp).read(0, 10, 65_536),
                    cluster.partitionLog(1, tp).read(0, 10, 65_536));
            assertEquals(3, cluster.snapshot(tp).highWatermark());
        }
    }


    private static long checkpoint(ClusterHarness cluster, TopicPartition tp, List<RecordData> records) {
        Messages.Reply reply = produce(cluster, tp, 1, 0, Acks.LEADER, records);
        assertEquals(ErrorCode.NONE, reply.error());
        assertEquals(new AppendResult(0, records.size()), ((Messages.ProduceBody) reply.body()).result());
        for (int brokerId : List.of(2, 3)) {
            assertEquals(records.size(), cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
            cluster.tracker(tp).report(brokerId, 0, records.size());
        }
        assertEquals(records.size(), cluster.tracker(tp).highWatermark());
        return cluster.partitionLog(1, tp).logEndOffset();
    }

    private static Messages.Reply produce(ClusterHarness cluster, TopicPartition tp, int brokerId, int epoch,
                                          Acks acks, List<RecordData> records) {
        try (RpcClient client = new RpcClient(cluster.endpoint(brokerId), 3_000)) {
            return client.call(new Messages.ProduceRequest(tp, epoch, acks, 2_000, records));
        }
    }

    private static List<RecordData> prefix() {
        return List.of(record("zero", 10), record("one", 11), record("two", 12));
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
