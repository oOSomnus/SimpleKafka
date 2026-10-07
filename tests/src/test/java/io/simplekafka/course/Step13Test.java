package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.values;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step13Test {
    @Test
    void flushesAtTheBatchThresholdAndReturnsAutomaticAndExplicitRanges() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                     new Partitioner(), 2)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            byte[] key = TestSupport.utf8("stable-key");

            producer.send("orders", key, TestSupport.utf8("first"), 10);
            producer.send("orders", key, TestSupport.utf8("second"), 11);
            assertEquals(List.of("first", "second"),
                    values(TestSupport.fetch(observer, tp, 0, 10, 4_096).records()),
                    "the batch is visible once its configured threshold is reached");

            producer.send("orders", key, TestSupport.utf8("third"), 12);
            assertEquals(List.of("first", "second"),
                    values(TestSupport.fetch(observer, tp, 0, 10, 4_096).records()),
                    "the third send flushes only the first two records");
            assertEquals(List.of(), TestSupport.fetch(observer, tp, 2, 10, 4_096).records(),
                    "the third record remains buffered until explicit flush");

            assertEquals(List.of(new ProduceReceipt(tp, 0, 2), new ProduceReceipt(tp, 2, 3)),
                    producer.flush());
            assertEquals(List.of("first", "second", "third"),
                    values(TestSupport.fetch(observer, tp, 0, 10, 4_096).records()));
            assertEquals(List.of(), producer.flush(), "a completed receipt is delivered only once");
        }
    }

    @Test
    void batchSizeOneMakesEverySendVisibleAndReturnsEachReceiptOnce() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                     new Partitioner(), 1)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            byte[] key = TestSupport.utf8("stable-key");

            producer.send("orders", key, TestSupport.utf8("one"), 1);
            assertEquals(List.of("one"), values(TestSupport.fetch(observer, tp, 0, 10, 4_096).records()));
            producer.send("orders", key, TestSupport.utf8("two"), 2);
            assertEquals(List.of("one", "two"), values(TestSupport.fetch(observer, tp, 0, 10, 4_096).records()));

            assertEquals(List.of(new ProduceReceipt(tp, 0, 1), new ProduceReceipt(tp, 1, 2)),
                    producer.flush());
            assertEquals(List.of(), producer.flush());
        }
    }

    @Test
    void automaticFlushForOnePartitionLeavesAnotherPartitionsPendingBatchInvisible() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 2);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                     new Partitioner(), 2)) {
            TopicPartition partitionZero = new TopicPartition("orders", 0);
            TopicPartition partitionOne = new TopicPartition("orders", 1);
            byte[] oneKey = TestSupport.utf8("123456789");

            producer.send("orders", null, TestSupport.utf8("pending-zero"), 1);
            assertEquals(List.of(), TestSupport.fetch(observer, partitionZero, 0, 10, 4_096).records());

            producer.send("orders", oneKey, TestSupport.utf8("one-first"), 2);
            producer.send("orders", oneKey, TestSupport.utf8("one-second"), 3);
            producer.send("orders", oneKey, TestSupport.utf8("one-third"), 4);
            assertEquals(List.of(), TestSupport.fetch(observer, partitionOne, 2, 10, 4_096).records(),
                    "the third same-key record remains buffered after the threshold batch");
            assertEquals(List.of(), TestSupport.fetch(observer, partitionZero, 0, 10, 4_096).records(),
                    "flushing partition one does not flush partition zero");
            assertEquals(List.of("one-first", "one-second"),
                    values(TestSupport.fetch(observer, partitionOne, 0, 10, 4_096).records()));

            List<ProduceReceipt> receipts = producer.flush();
            assertEquals(3, receipts.size());
            assertEquals(List.of(
                            new ProduceReceipt(partitionOne, 0, 2),
                            new ProduceReceipt(partitionOne, 2, 3)),
                    receipts.stream().filter(receipt -> receipt.tp().equals(partitionOne)).toList(),
                    "the known CRC32C vector keeps both batches for the same key on partition one");
            assertEquals(List.of(new ProduceReceipt(partitionZero, 0, 1)),
                    receipts.stream().filter(receipt -> receipt.tp().equals(partitionZero)).toList());
            assertEquals(List.of("pending-zero"),
                    values(TestSupport.fetch(observer, partitionZero, 0, 10, 4_096).records()));
            assertEquals(List.of("one-first", "one-second", "one-third"),
                    values(TestSupport.fetch(observer, partitionOne, 0, 10, 4_096).records()));
            assertEquals(List.of(), producer.flush());
        }
    }

    @Test
    void rejectsInvalidSendsWithoutPersistingAnyRecords() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000)) {
            TopicPartition tp = new TopicPartition("orders", 0);

            try (SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                    new Partitioner(), 1)) {
                assertCode(ErrorCode.INVALID_REQUEST, () -> producer.send("orders", null, null, 0));
            }
            assertEquals(List.of(), TestSupport.fetch(observer, tp, 0, 10, 4_096).records(),
                    "null value rejection must not append");

            try (SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                    new Partitioner(), 1)) {
                assertCode(ErrorCode.INVALID_REQUEST,
                        () -> producer.send("orders", null, TestSupport.utf8("negative-time"), -1));
            }
            assertEquals(List.of(), TestSupport.fetch(observer, tp, 0, 10, 4_096).records(),
                    "negative timestamp rejection must not append");

            try (SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                    new Partitioner(), 1)) {
                assertCode(ErrorCode.INVALID_REQUEST,
                        () -> producer.send("orders", null, new byte[1_048_549], 0));
            }
            assertEquals(List.of(), TestSupport.fetch(observer, tp, 0, 10, 4_096).records(),
                    "oversized record rejection must not append");

            try (SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                    new Partitioner(), 1)) {
                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        () -> producer.send("missing", null, TestSupport.utf8("unknown"), 0));
            }
            assertEquals(List.of(), TestSupport.fetch(observer, tp, 0, 10, 4_096).records(),
                    "unknown-topic rejection must not append to an existing partition");
        }
    }

    @Test
    void keepsTheProducerPoisonedAfterAnAutomaticFlushTimesOut() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(client, new Partitioner(), 2)) {
            producer.send("orders", null, TestSupport.utf8("pending-0"), 1);
            broker.close();

            assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send("orders", null, TestSupport.utf8("pending-1"), 2));
            assertCode(ErrorCode.REQUEST_TIMEOUT, () -> producer.send("orders", null, TestSupport.utf8("must-not-retry"), 3));
            assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
        }
    }

    @Test
    void doesNotRetryAProduceWhoseAppendSucceededBeforeItsResponseTimedOut() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             DelayedProduceBroker broker = new DelayedProduceBroker(temp.root());
             SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 250),
                     new Partitioner(), 1)) {
            TopicPartition tp = new TopicPartition("orders", 0);

            assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send("orders", null, TestSupport.utf8("persisted-once"), 1));
            assertTrue(broker.awaitProduce(2, TimeUnit.SECONDS), "produce handler must append before delaying its reply");
            assertOnlyRecord(broker.catalog().partition(tp), "persisted-once");

            assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send("orders", null, TestSupport.utf8("must-not-be-appended"), 2));
            assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
            assertOnlyRecord(broker.catalog().partition(tp), "persisted-once");
        }
    }

    private static void assertOnlyRecord(PartitionLog log, String expectedValue) {
        List<LogRecord> records = log.read(0, 10, 4_096);
        assertEquals(List.of(0L), records.stream().map(LogRecord::offset).toList());
        assertEquals(List.of(expectedValue), values(records));
        assertEquals(1, log.logEndOffset());
    }

    private static final class DelayedProduceBroker implements AutoCloseable {
        private final CountDownLatch appended = new CountDownLatch(1);
        private final CountDownLatch release = new CountDownLatch(1);
        private final BrokerServer server;
        private final Endpoint endpoint;
        private final PartitionCatalog catalog;

        private DelayedProduceBroker(java.nio.file.Path root) {
            AtomicReference<BrokerHandler> handler = new AtomicReference<>();
            server = new BrokerServer("127.0.0.1", 0, request -> {
                BrokerHandler current = handler.get();
                if (current == null) return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "broker is not ready");
                Messages.Reply reply = current.handle(request);
                if (request instanceof Messages.ProduceRequest && reply.error() == ErrorCode.NONE) {
                    appended.countDown();
                    try {
                        release.await(2, TimeUnit.SECONDS);
                    } catch (InterruptedException exception) {
                        Thread.currentThread().interrupt();
                    }
                }
                return reply;
            });

            PartitionCatalog openedCatalog = null;
            try {
                endpoint = server.start();
                openedCatalog = new PartitionCatalog(root, 1, endpoint);
                openedCatalog.createTopic("orders", 1);
                handler.set(new BrokerHandler(openedCatalog, null, null, null));
                catalog = openedCatalog;
            } catch (RuntimeException failure) {
                release.countDown();
                server.close();
                if (openedCatalog != null) openedCatalog.close();
                throw failure;
            }
        }

        private Endpoint endpoint() {
            return endpoint;
        }

        private PartitionCatalog catalog() {
            return catalog;
        }

        private boolean awaitProduce(long timeout, TimeUnit unit) throws InterruptedException {
            return appended.await(timeout, unit);
        }

        @Override
        public void close() {
            release.countDown();
            server.close();
            catalog.close();
        }
    }
}
