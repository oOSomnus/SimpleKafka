package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.values;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step13Test {
    @Test
    void batchesByPartitionAndReturnsAutomaticAndExplicitFlushReceipts() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 7);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(client, new Partitioner(), 2)) {
            byte[] stableKey = "123456789".getBytes(StandardCharsets.US_ASCII);
            producer.send("orders", stableKey, TestSupport.utf8("key-0"), 10);
            producer.send("orders", stableKey, TestSupport.utf8("key-1"), 11);
            producer.send("orders", null, TestSupport.utf8("null-0"), 12);
            producer.send("orders", null, TestSupport.utf8("null-1"), 13);

            List<ProduceReceipt> receipts = producer.flush();
            assertEquals(3, receipts.size());
            Map<TopicPartition, AppendResult> ranges = new HashMap<>();
            for (ProduceReceipt receipt : receipts) {
                ranges.put(receipt.tp(), new AppendResult(receipt.firstOffset(), receipt.nextOffset()));
            }
            assertEquals(new AppendResult(0, 2), ranges.get(new TopicPartition("orders", 2)),
                    "the CRC32C check vector routes to partition 2 of 7");
            assertEquals(new AppendResult(0, 1), ranges.get(new TopicPartition("orders", 0)));
            assertEquals(new AppendResult(0, 1), ranges.get(new TopicPartition("orders", 1)));
            assertEquals(List.of(), producer.flush());

            assertEquals(List.of("key-0", "key-1"), values(TestSupport.fetch(client,
                    new TopicPartition("orders", 2), 0, 10, 4_096).records()));
            assertEquals(List.of("null-0"), values(TestSupport.fetch(client,
                    new TopicPartition("orders", 0), 0, 10, 4_096).records()));
            assertEquals(List.of("null-1"), values(TestSupport.fetch(client,
                    new TopicPartition("orders", 1), 0, 10, 4_096).records()));
        }
    }
    @Test
    void treatsATimedOutAutomaticBatchAsUnknownAndNeverRetriesIt() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(client, new Partitioner(), 2)) {
            producer.send("orders", null, TestSupport.utf8("pending-0"), 1);
            broker.close();

            assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send("orders", null, TestSupport.utf8("pending-1"), 2));
            assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send("orders", null, TestSupport.utf8("must-not-retry"), 3));
            assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
        }
    }
}
