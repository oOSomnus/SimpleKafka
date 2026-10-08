package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.model.Acks;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
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
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.values;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
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
    void routesNullKeysWithinEachTopicAcrossDifferentPartitionCounts() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "large", 3);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000);
             SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                     new Partitioner(), 10)) {
            broker.catalog().createTopic("small", 1);
            TopicPartition largeZero = new TopicPartition("large", 0);
            TopicPartition largeOne = new TopicPartition("large", 1);
            TopicPartition smallZero = new TopicPartition("small", 0);

            producer.send("large", null, TestSupport.utf8("large-zero"), 1);
            producer.send("large", null, TestSupport.utf8("large-one"), 2);
            producer.send("small", null, TestSupport.utf8("small-zero"), 3);

            assertEquals(List.of(
                            new ProduceReceipt(largeZero, 0, 1),
                            new ProduceReceipt(largeOne, 0, 1),
                            new ProduceReceipt(smallZero, 0, 1)),
                    producer.flush());
            assertEquals(List.of("large-zero"),
                    values(TestSupport.fetch(observer, largeZero, 0, 10, 4_096).records()));
            assertEquals(List.of("large-one"),
                    values(TestSupport.fetch(observer, largeOne, 0, 10, 4_096).records()));
            assertEquals(List.of("small-zero"),
                    values(TestSupport.fetch(observer, smallZero, 0, 10, 4_096).records()));
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

            try (SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                    new Partitioner(), 1)) {
                assertCode(ErrorCode.INVALID_REQUEST, () -> producer.send("orders", null, null, 0));
                assertCode(ErrorCode.INVALID_REQUEST,
                        () -> producer.send("orders", null, TestSupport.utf8("negative-time"), -1));
                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        () -> producer.send("missing", null, TestSupport.utf8("discard"), 0));

                producer.send("orders", null, TestSupport.utf8("valid"), 10);
                assertEquals(List.of(new ProduceReceipt(tp, 0, 1)), producer.flush());
                assertEquals(List.of(new LogRecord(0, new RecordData(null, TestSupport.utf8("valid"), 10))),
                        TestSupport.fetch(observer, tp, 0, 10, 4_096).records());

                broker.catalog().createTopic("missing", 1);
                TopicPartition created = new TopicPartition("missing", 0);
                producer.send("missing", null, TestSupport.utf8("created"), 11);
                assertEquals(List.of(new ProduceReceipt(created, 0, 1)), producer.flush());
                assertEquals(List.of(new LogRecord(0, new RecordData(null, TestSupport.utf8("created"), 11))),
                        TestSupport.fetch(observer, created, 0, 10, 4_096).records());
            }
        }
    }

    @Test
    void directRpcClientCannotBeReusedAfterClosingAfterARecordOperation() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000);
             RpcClient owner = new RpcClient(broker.endpoint(), 3_000)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            RecordData data = new RecordData(null, TestSupport.utf8("written"), 10);
            Messages.Reply reply = owner.call(new Messages.ProduceRequest(
                    tp, 0, Acks.LEADER, 1_000, List.of(data)));
            assertEquals(ErrorCode.NONE, reply.error());
            assertEquals(new io.simplekafka.model.AppendResult(0, 1),
                    ((Messages.ProduceBody) reply.body()).result());
            assertEquals(List.of(new LogRecord(0, data)), TestSupport.fetch(observer, tp, 0, 10, 4_096).records());

            owner.close();
            assertThrows(IllegalStateException.class,
                    () -> owner.call(new Messages.MetadataRequest("orders")));
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

    @Test
    void makesDirectProducerOutcomeUnknownAfterEveryAppendError() throws Exception {
        for (ErrorCode error : List.of(
                ErrorCode.FENCED_EPOCH,
                ErrorCode.NOT_ENOUGH_REPLICAS,
                ErrorCode.NOT_LEADER,
                ErrorCode.STORAGE_ERROR)) {
            assertDirectAppendFailurePoisonsProducer(error, true);
            assertDirectAppendFailurePoisonsProducer(error, false);
        }
    }

    private static void assertDirectAppendFailurePoisonsProducer(ErrorCode error, boolean automatic)
            throws Exception {
        try (TempDirectory temp = new TempDirectory();
             AppendThenErrorBroker broker = new AppendThenErrorBroker(temp.root(), error);
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            SimpleProducer producer = new SimpleProducer(new RpcClient(broker.endpoint(), 3_000),
                    new Partitioner(), automatic ? 1 : 2);
            try {
                if (automatic) {
                    assertCode(error,
                            () -> producer.send(tp.topic(), null, TestSupport.utf8("old"), 1));
                } else {
                    producer.send(tp.topic(), null, TestSupport.utf8("old"), 1);
                    assertCode(error, producer::flush);
                }

                for (int repetition = 0; repetition < 2; repetition++) {
                    assertCode(ErrorCode.REQUEST_TIMEOUT,
                            () -> producer.send(tp.topic(), null, TestSupport.utf8("must-not-append"), 2));
                    assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
                    assertCode(ErrorCode.REQUEST_TIMEOUT, () -> producer.setAcks(Acks.LEADER, 5_000));
                }
            } finally {
                producer.close();
            }

            LogRecord old = new LogRecord(0, new RecordData(null, TestSupport.utf8("old"), 1));
            assertEquals(List.of(old), TestSupport.fetch(observer, tp, 0, 10, 4_096).records());
            assertEquals(1, broker.catalog().partition(tp).logEndOffset());

            try (SimpleProducer replacement = new SimpleProducer(
                    new RpcClient(broker.endpoint(), 3_000), new Partitioner(), 1)) {
                replacement.send(tp.topic(), null, TestSupport.utf8("new"), 3);
                assertEquals(List.of(new ProduceReceipt(tp, 1, 2)), replacement.flush());
            }
            assertEquals(List.of(old, new LogRecord(1, new RecordData(null, TestSupport.utf8("new"), 3))),
                    TestSupport.fetch(observer, tp, 0, 10, 4_096).records());
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

    private static final class AppendThenErrorBroker implements AutoCloseable {
        private final BrokerServer server;
        private final Endpoint endpoint;
        private final PartitionCatalog catalog;

        private AppendThenErrorBroker(java.nio.file.Path root, ErrorCode error) {
            AtomicReference<BrokerHandler> handler = new AtomicReference<>();
            AtomicBoolean errorReturned = new AtomicBoolean();
            server = new BrokerServer("127.0.0.1", 0, request -> {
                BrokerHandler current = handler.get();
                if (current == null)
                    return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "broker is not ready");
                Messages.Reply reply = current.handle(request);
                if (request instanceof Messages.ProduceRequest && reply.error() == ErrorCode.NONE
                        && errorReturned.compareAndSet(false, true))
                    return Messages.Reply.failure(error, "injected post-append error");
                return reply;
            });

            Endpoint openedEndpoint = server.start();
            PartitionCatalog openedCatalog = null;
            try {
                openedCatalog = new PartitionCatalog(root, 1, openedEndpoint);
                openedCatalog.createTopic("orders", 1);
                handler.set(new BrokerHandler(openedCatalog, null, null, null));
            } catch (RuntimeException failure) {
                server.close();
                if (openedCatalog != null) openedCatalog.close();
                throw failure;
            }
            endpoint = openedEndpoint;
            catalog = openedCatalog;
        }

        private Endpoint endpoint() {
            return endpoint;
        }

        private PartitionCatalog catalog() {
            return catalog;
        }

        @Override
        public void close() {
            server.close();
            catalog.close();
        }
    }

}
