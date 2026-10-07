package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step21Test {
    @TempDir Path root;

    @Test
    void followerPullCopiesContiguousDurableRecordsAndRejectsWrongRolesAndEpochs() {
        AtomicLong now = new AtomicLong(10);
        TimeSource clock = now::get;
        TopicPartition tp = new TopicPartition("step21", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<RecordData> batch = List.of(record("first", 11), record("second", 12), record("third", 13));
            assertEquals(0, cluster.partitionLog(1, tp).append(batch).firstOffset());

            assertEquals(2, cluster.replicator(2, tp).pollOnce(2, 16_384));
            assertEquals(2, cluster.partitionLog(2, tp).logEndOffset());
            assertEquals(1, cluster.replicator(2, tp).pollOnce(2, 16_384));
            assertEquals(0, cluster.replicator(2, tp).pollOnce(2, 16_384));

            List<LogRecord> expected = cluster.partitionLog(1, tp).read(0, 10, 16_384);
            assertEquals(expected, cluster.partitionLog(2, tp).read(0, 10, 16_384));
            assertEquals(3, cluster.partitionLog(2, tp).logEndOffset());

            CourseException invalidLimits = assertThrows(CourseException.class,
                    () -> cluster.replicator(2, tp).pollOnce(0, 16_384));
            assertEquals(ErrorCode.INVALID_REQUEST, invalidLimits.code());
            assertEquals(expected, cluster.partitionLog(2, tp).read(0, 10, 16_384));

            Messages.ReplicaFetchRequest staleEpoch = new Messages.ReplicaFetchRequest(tp, 2, 1, 0, 10, 16_384, false);
            try (RpcClient client = new RpcClient(cluster.endpoint(1), 2_000)) {
                assertEquals(ErrorCode.FENCED_EPOCH, client.call(staleEpoch).error());
            }
            Messages.ReplicaFetchRequest servedByFollower = new Messages.ReplicaFetchRequest(tp, 2, 0, 0, 10, 16_384, false);
            try (RpcClient client = new RpcClient(cluster.endpoint(3), 2_000)) {
                assertEquals(ErrorCode.NOT_LEADER, client.call(servedByFollower).error());
            }
            assertEquals(expected, cluster.partitionLog(2, tp).read(0, 10, 16_384));
        }
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, value.getBytes(StandardCharsets.UTF_8), timestamp);
    }
}
