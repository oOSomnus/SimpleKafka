package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.model.Acks;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step27Test {
    @TempDir Path root;

    @Test
    void routerRefreshesThroughSurvivingBootstrapAndPropagatesEpochErrorsWithoutResending() {
        TimeSource clock = new AtomicLong(0)::get;
        TopicPartition tp = new TopicPartition("step27", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, clock)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<io.simplekafka.model.Endpoint> bootstrap = List.of(
                    cluster.endpoint(1), cluster.endpoint(2), cluster.endpoint(3));
            RpcClientFactory clients = endpoint -> new RpcClient(endpoint, 2_000);
            try (MetadataRouter router = new MetadataRouter(bootstrap, clients);
                 SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1);
                 SimpleConsumer consumer = new SimpleConsumer(router, "step27-reader")) {
                producer.send(tp.topic(), null, bytes("before-failover"), 10);
                List<ProduceReceipt> firstReceipts = producer.flush();
                assertEquals(1, firstReceipts.size());
                assertEquals(0, firstReceipts.get(0).firstOffset());
                assertEquals(1, firstReceipts.get(0).nextOffset());

                for (int brokerId : List.of(2, 3)) {
                    assertEquals(1, cluster.replicator(brokerId, tp).pollOnce(10, 65_536));
                    cluster.tracker(tp).report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
                }
                assertEquals(1, cluster.tracker(tp).highWatermark());
                consumer.assign(List.of(tp));
                Map<TopicPartition, List<LogRecord>> observed = consumer.poll(10, 65_536);
                assertEquals(List.of(new LogRecord(0, record("before-failover", 10))), observed.get(tp));

                cluster.stopBroker(1);
                var elected = cluster.elect(tp);
                assertEquals(2, elected.leaderId());
                assertEquals(1, elected.epoch());
                router.refresh(tp.topic());
                assertEquals(cluster.endpoint(2), router.activeBootstrap());
                assertEquals(2, router.metadata(tp.topic()).get(0).leaderId());

                Messages.Reply staleEpoch = router.call(tp, new Messages.ProduceRequest(tp, 0, Acks.LEADER,
                        2_000, List.of(record("must-not-be-retried", 11))));
                assertEquals(ErrorCode.FENCED_EPOCH, staleEpoch.error());
                assertEquals(1, cluster.partitionLog(2, tp).logEndOffset(),
                        "the rejected request must not append as a hidden retry");
                assertNotNull(router.metadata(tp.topic()).get(0));
                assertEquals(1, router.metadata(tp.topic()).get(0).epoch());

                producer.send(tp.topic(), null, bytes("explicit-next-request"), 12);
                List<ProduceReceipt> nextReceipts = producer.flush();
                assertEquals(1, nextReceipts.size());
                assertEquals(1, nextReceipts.get(0).firstOffset());
                assertEquals(2, nextReceipts.get(0).nextOffset());
                assertEquals(List.of(record("before-failover", 10), record("explicit-next-request", 12)),
                        cluster.partitionLog(2, tp).read(0, 10, 65_536).stream().map(LogRecord::data).toList());
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, bytes(value), timestamp);
    }
}
