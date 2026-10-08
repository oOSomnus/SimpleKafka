package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.assertNull;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.client.GroupConsumer;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicLong;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.io.TempDir;

class Step27Test {
    @TempDir Path root;

    @Test
    void refreshSkipsUnavailableAndFactoryThrowingBootstrapsBeforeCachingMetadata() {
        TopicPartition tp = new TopicPartition("step27-bootstrap", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Endpoint unavailable = new Endpoint("127.0.0.1", 0);
            Endpoint factoryFailure = new Endpoint("factory-throws.invalid", 9_091);
            List<Endpoint> bootstrap = List.of(unavailable, factoryFailure, cluster.endpoint(1));
            RpcClientFactory clients = endpoint -> {
                if (endpoint.equals(factoryFailure)) throw new IllegalStateException("connection factory failed");
                return new RpcClient(endpoint, 500);
            };

            try (MetadataRouter router = new MetadataRouter(bootstrap, clients)) {
                router.refresh(tp.topic());
                assertEquals(cluster.endpoint(1), router.activeBootstrap());
                assertEquals(1, router.metadata(tp.topic()).size());
                assertEquals(tp, router.metadata(tp.topic()).get(0).tp());
                assertEquals(cluster.endpoint(1), router.metadata(tp.topic()).get(0).leader());
            }
        }
    }

    @Test
    void allUnavailableBootstrapsProduceNoSyntheticMetadataOrActiveEndpoint() {
        List<Endpoint> bootstrap = List.of(
                new Endpoint("127.0.0.1", 0), new Endpoint("127.0.0.1", 0));
        try (MetadataRouter router = new MetadataRouter(bootstrap,
                endpoint -> new RpcClient(endpoint, 250))) {
            TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT, () -> router.refresh("step27-unavailable"));
            assertNull(router.activeBootstrap());
            TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> router.metadata("step27-unavailable"));
            assertNull(router.activeBootstrap(), "failed initial discovery cannot install a synthetic route");
        }
    }


    @Test
    void failedRefreshTimesOutWithoutReplacingCacheAndLaterSuccessfulRefreshReplacesIt() {
        TopicPartition tp = new TopicPartition("step27-refresh", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            Endpoint firstBootstrap = cluster.endpoint(1);
            List<Endpoint> bootstrap = List.of(firstBootstrap, cluster.endpoint(2), cluster.endpoint(3));
            AtomicBoolean failOtherFactories = new AtomicBoolean();
            RpcClientFactory clients = endpoint -> {
                if (failOtherFactories.get() && !endpoint.equals(firstBootstrap))
                    throw new IllegalStateException("bootstrap factory unavailable");
                return new RpcClient(endpoint, 500);
            };

            try (MetadataRouter router = new MetadataRouter(bootstrap, clients)) {
                PartitionMetadata cached = router.metadata(tp.topic()).get(0);
                assertEquals(firstBootstrap, router.activeBootstrap());

                cluster.stopBroker(1);
                assertEquals(2, cluster.elect(tp).leaderId());
                failOtherFactories.set(true);
                TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT, () -> router.refresh(tp.topic()));
                assertEquals(cached, router.metadata(tp.topic()).get(0),
                        "a failed refresh must leave the last successful route available");
                assertEquals(firstBootstrap, router.activeBootstrap());

                failOtherFactories.set(false);
                router.refresh(tp.topic());
                PartitionMetadata refreshed = router.metadata(tp.topic()).get(0);
                assertEquals(2, refreshed.leaderId());
                assertEquals(1, refreshed.epoch());
                assertEquals(cluster.endpoint(2), refreshed.leader());
                assertEquals(cluster.endpoint(2), router.activeBootstrap());
            }
        }
    }

    @Test
    void returnsLeaderErrorsUnchangedAndRefreshesOnlyForTheNextRequest() {
        TopicPartition tp = new TopicPartition("step27-errors", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            try (MetadataRouter router = new MetadataRouter(List.of(cluster.endpoint(3)),
                    endpoint -> new RpcClient(endpoint, 1_000))) {
                router.refresh(tp.topic());
                PartitionMetadata initial = router.metadata(tp.topic()).get(0);

                Messages.ProduceRequest staleEpoch = new Messages.ProduceRequest(tp, initial.epoch() + 1,
                        Acks.LEADER, 1_000, List.of(record("fenced", 10)));
                Messages.Reply expectedFenced;
                try (RpcClient direct = new RpcClient(initial.leader(), 1_000)) {
                    expectedFenced = direct.call(staleEpoch);
                }
                assertEquals(ErrorCode.FENCED_EPOCH, expectedFenced.error());
                assertEquals(expectedFenced, router.call(tp, staleEpoch));
                assertEquals(initial, router.metadata(tp.topic()).get(0));
                assertEquals(0, cluster.partitionLog(1, tp).logEndOffset());

                Messages.Reply afterFence = router.call(tp, new Messages.ProduceRequest(tp, initial.epoch(),
                        Acks.LEADER, 1_000, List.of(record("after-fence", 11))));
                assertEquals(ErrorCode.NONE, afterFence.error());
                assertEquals(new AppendResult(0, 1), ((Messages.ProduceBody) afterFence.body()).result());

                cluster.authority().setBrokerOnline(1, false);
                PartitionMetadata elected = cluster.elect(tp);
                assertEquals(2, elected.leaderId());
                assertEquals(1, elected.epoch());
                Messages.ProduceRequest oldLeader = new Messages.ProduceRequest(tp, elected.epoch(),
                        Acks.LEADER, 1_000, List.of(record("not-leader", 12)));
                Messages.Reply expectedNotLeader;
                try (RpcClient direct = new RpcClient(initial.leader(), 1_000)) {
                    expectedNotLeader = direct.call(oldLeader);
                }
                assertEquals(ErrorCode.NOT_LEADER, expectedNotLeader.error());
                assertEquals(expectedNotLeader, router.call(tp, oldLeader));
                assertEquals(elected, router.metadata(tp.topic()).get(0),
                        "the business error is returned, while metadata is refreshed for a later call");
                assertEquals(0, cluster.partitionLog(2, tp).logEndOffset());

                Messages.Reply afterNotLeader = router.call(tp, new Messages.ProduceRequest(tp, elected.epoch(),
                        Acks.LEADER, 1_000, List.of(record("after-not-leader", 13))));
                assertEquals(ErrorCode.NONE, afterNotLeader.error());
                assertEquals(List.of(record("after-not-leader", 13)),
                        cluster.partitionLog(2, tp).read(0, 10, 4_096).stream().map(LogRecord::data).toList());

                PartitionMetadata lastSuccessful = router.metadata(tp.topic()).get(0);
                cluster.stopBroker(3);
                Messages.ProduceRequest fencedWhileRefreshUnavailable = new Messages.ProduceRequest(tp,
                        lastSuccessful.epoch() + 1, Acks.LEADER, 1_000, List.of(record("not-appended", 14)));
                Messages.Reply expectedUnavailableRefresh;
                try (RpcClient direct = new RpcClient(lastSuccessful.leader(), 1_000)) {
                    expectedUnavailableRefresh = direct.call(fencedWhileRefreshUnavailable);
                }
                assertEquals(ErrorCode.FENCED_EPOCH, expectedUnavailableRefresh.error());
                assertEquals(expectedUnavailableRefresh, router.call(tp, fencedWhileRefreshUnavailable));
                assertEquals(lastSuccessful, router.metadata(tp.topic()).get(0),
                        "a failed metadata refresh must not replace a successful cache entry");
                assertEquals(1, cluster.partitionLog(2, tp).logEndOffset());
            }
        }
    }

    @Test
    void routerDoesNotReplayAProduceAfterItsAppendResponseTimesOut() throws Exception {
        CountDownLatch appended = new CountDownLatch(1);
        CountDownLatch release = new CountDownLatch(1);
        try (TestBroker broker = new TestBroker(root.resolve("delayed"), (handler, request) -> {
            Messages.Reply reply = handler.handle(request);
            if (request instanceof Messages.ProduceRequest && reply.error() == ErrorCode.NONE) {
                appended.countDown();
                try {
                    release.await(2, TimeUnit.SECONDS);
                } catch (InterruptedException exception) {
                    Thread.currentThread().interrupt();
                }
            }
            return reply;
        }, release::countDown);
             MetadataRouter router = new MetadataRouter(List.of(broker.endpoint()),
                     endpoint -> new RpcClient(endpoint, 250));
             SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1)) {
            TopicPartition tp = new TopicPartition(TestBroker.TOPIC, 0);
            TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send(tp.topic(), null, bytes("persisted-once"), 10));
            assertTrue(appended.await(2, TimeUnit.SECONDS), "the server must append before delaying its reply");
            assertEquals(List.of(new LogRecord(0, record("persisted-once", 10))),
                    broker.catalog().partition(tp).read(0, 10, 4_096));
            assertEquals(1, broker.catalog().partition(tp).logEndOffset());

            TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT,
                    () -> producer.send(tp.topic(), null, bytes("must-not-replay"), 11));
            TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
            assertEquals(List.of(new LogRecord(0, record("persisted-once", 10))),
                    broker.catalog().partition(tp).read(0, 10, 4_096));
            assertEquals(1, broker.catalog().partition(tp).logEndOffset());
        }
    }

    @Test
    void makesRoutedProducerOutcomeUnknownAfterEveryAppendError() {
        for (ErrorCode error : List.of(
                ErrorCode.FENCED_EPOCH,
                ErrorCode.NOT_ENOUGH_REPLICAS,
                ErrorCode.NOT_LEADER,
                ErrorCode.STORAGE_ERROR)) {
            assertRoutedAppendFailurePoisonsProducer(error, true);
            assertRoutedAppendFailurePoisonsProducer(error, false);
        }
    }

    private void assertRoutedAppendFailurePoisonsProducer(ErrorCode error, boolean automatic) {
        TopicPartition tp = new TopicPartition(TestBroker.TOPIC, 0);
        AtomicBoolean errorReturned = new AtomicBoolean();
        String suffix = error.name() + (automatic ? "-automatic" : "-explicit");
        try (TestBroker broker = new TestBroker(root.resolve("append-error-" + suffix), (handler, request) -> {
            Messages.Reply reply = handler.handle(request);
            if (request instanceof Messages.ProduceRequest && reply.error() == ErrorCode.NONE
                    && errorReturned.compareAndSet(false, true))
                return Messages.Reply.failure(error, "injected post-append error");
            return reply;
        }, () -> { });
             MetadataRouter router = new MetadataRouter(List.of(broker.endpoint()),
                     endpoint -> new RpcClient(endpoint, 3_000));
             RpcClient observer = new RpcClient(broker.endpoint(), 3_000)) {
            router.refresh(tp.topic());
            SimpleProducer producer = new SimpleProducer(router, new Partitioner(), automatic ? 1 : 2);
            try {
                if (automatic) {
                    TestSupport.assertCode(error,
                            () -> producer.send(tp.topic(), null, bytes("old"), 10));
                } else {
                    producer.send(tp.topic(), null, bytes("old"), 10);
                    TestSupport.assertCode(error, producer::flush);
                }

                for (int repetition = 0; repetition < 2; repetition++) {
                    TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT,
                            () -> producer.send(tp.topic(), null, bytes("must-not-append"), 11));
                    TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
                    TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT,
                            () -> producer.setAcks(Acks.LEADER, 5_000));
                }
            } finally {
                producer.close();
            }

            LogRecord old = new LogRecord(0, record("old", 10));
            assertEquals(List.of(old), TestSupport.fetch(observer, tp, 0, 10, 4_096).records());
            assertEquals(1, broker.catalog().partition(tp).logEndOffset());

            try (SimpleProducer replacement = new SimpleProducer(router, new Partitioner(), 1)) {
                replacement.send(tp.topic(), null, bytes("new"), 12);
                assertEquals(List.of(new ProduceReceipt(tp, 1, 2)), replacement.flush());
            }
            assertEquals(List.of(old, new LogRecord(1, record("new", 12))),
                    TestSupport.fetch(observer, tp, 0, 10, 4_096).records());
        }
    }

    @Test
    void routerDoesNotReplayStorageOrTransportFailures() {
        AtomicBoolean failFirstProduce = new AtomicBoolean(true);
        try (TestBroker broker = new TestBroker(root.resolve("storage-failure"), (handler, request) -> {
            if (request instanceof Messages.ProduceRequest && failFirstProduce.compareAndSet(true, false))
                return Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "injected storage failure");
            return handler.handle(request);
        }, () -> { });
             MetadataRouter router = new MetadataRouter(List.of(broker.endpoint()),
                     endpoint -> new RpcClient(endpoint, 1_000));
             SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1)) {
            TopicPartition tp = new TopicPartition(TestBroker.TOPIC, 0);
            TestSupport.assertCode(ErrorCode.STORAGE_ERROR,
                    () -> producer.send(tp.topic(), null, bytes("storage-error"), 20));
            assertEquals(0, broker.catalog().partition(tp).logEndOffset(),
                    "a storage error must not trigger an implicit second produce");
        }

        TopicPartition tp = new TopicPartition("step27-transport", 0);
        try (ClusterHarness cluster = new ClusterHarness(root.resolve("transport"),
                new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            List<Endpoint> bootstrap = List.of(cluster.endpoint(1), cluster.endpoint(2), cluster.endpoint(3));
            try (MetadataRouter router = new MetadataRouter(bootstrap,
                    endpoint -> new RpcClient(endpoint, 500));
                 SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1)) {
                router.refresh(tp.topic());
                cluster.stopBroker(1);
                assertEquals(2, cluster.elect(tp).leaderId());

                TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT,
                        () -> producer.send(tp.topic(), null, bytes("transport-error"), 21));
                for (int brokerId : ClusterHarness.BROKER_IDS)
                    assertEquals(0, cluster.partitionLog(brokerId, tp).logEndOffset(),
                            "a failed request must not be replayed to another broker");
                TestSupport.assertCode(ErrorCode.REQUEST_TIMEOUT, producer::flush);
            }
        }
    }

    @Test
    void keepsTopicRoutesIndependentAcrossFailoverAndRestartAndRoutesAllClientTypes() {
        TopicPartition routeA = new TopicPartition("route-a", 0);
        TopicPartition routeB = new TopicPartition("route-b", 0);
        try (ClusterHarness cluster = new ClusterHarness(root, new AtomicLong(0)::get)) {
            cluster.start();
            cluster.createTopic(routeA.topic(), 1, 1);
            cluster.createTopic(routeB.topic(), 1, 2);
            Endpoint originalBroker1 = cluster.endpoint(1);
            Endpoint originalBroker2 = cluster.endpoint(2);
            List<Endpoint> bootstrap = List.of(originalBroker1, originalBroker2, cluster.endpoint(3));

            try (MetadataRouter router = new MetadataRouter(bootstrap,
                    endpoint -> new RpcClient(endpoint, 1_000));
                 SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1);
                 SimpleConsumer consumer = new SimpleConsumer(router, "step27-manual");
                 GroupConsumer groupConsumer = new GroupConsumer(router, "step27-group", "member-a")) {
                router.refresh(routeA.topic());
                router.refresh(routeB.topic());
                assertEquals(1, router.metadata(routeA.topic()).get(0).leaderId());
                assertEquals(1, router.metadata(routeB.topic()).get(0).leaderId());

                cluster.stopBroker(1);
                assertEquals(2, cluster.elect(routeA).leaderId());
                PartitionMetadata unaffected = cluster.authority().metadata(routeB.topic()).get(0);
                assertEquals(1, unaffected.leaderId());
                assertEquals(0, unaffected.epoch());

                cluster.restartBroker(1);
                Endpoint restartedBroker1 = cluster.endpoint(1);
                assertNotEquals(originalBroker1, restartedBroker1);
                assertEquals(originalBroker1, router.metadata(routeB.topic()).get(0).leader(),
                        "an unrefreshed route retains its cached endpoint");
                assertEquals(1, cluster.authority().metadata(routeB.topic()).get(0).leaderId());
                assertEquals(0, cluster.authority().metadata(routeB.topic()).get(0).epoch());

                router.refresh(routeA.topic());
                router.refresh(routeB.topic());
                assertEquals(2, router.metadata(routeA.topic()).get(0).leaderId());
                assertEquals(cluster.endpoint(2), router.metadata(routeA.topic()).get(0).leader());
                assertEquals(1, router.metadata(routeB.topic()).get(0).leaderId());
                assertEquals(restartedBroker1, router.metadata(routeB.topic()).get(0).leader());

                producer.send(routeA.topic(), null, bytes("route-a-before-restart"), 30);
                producer.send(routeB.topic(), null, bytes("route-b-independent"), 31);
                assertEquals(List.of(new ProduceReceipt(routeA, 0, 1), new ProduceReceipt(routeB, 0, 1)),
                        producer.flush());

                assertEquals(1, cluster.snapshot(routeA).highWatermark(),
                        "the elected single-replica ISR commits route-a with minISR one");
                for (int brokerId : List.of(2, 3)) {
                    assertEquals(1, cluster.replicateOnce(brokerId, routeB, 10, 4_096));
                    cluster.tracker(routeB).report(brokerId, 0, 1);
                }
                assertEquals(1, cluster.snapshot(routeB).highWatermark(),
                        "route-b becomes consumer-visible only after both ISR followers report");
                assertEquals(List.of(record("route-a-before-restart", 30)),
                        cluster.partitionLog(2, routeA).read(0, 10, 4_096).stream().map(LogRecord::data).toList());
                assertEquals(List.of(record("route-b-independent", 31)),
                        cluster.partitionLog(1, routeB).read(0, 10, 4_096).stream().map(LogRecord::data).toList());
                assertEquals(0, cluster.partitionLog(1, routeA).logEndOffset());
                assertEquals(List.of(record("route-b-independent", 31)),
                        cluster.partitionLog(2, routeB).read(0, 10, 4_096).stream().map(LogRecord::data).toList());

                consumer.assign(List.of(routeA, routeB));
                Map<TopicPartition, List<LogRecord>> firstPoll = consumer.poll(10, 4_096);
                assertEquals(List.of(new LogRecord(0, record("route-a-before-restart", 30))),
                        firstPoll.get(routeA));
                assertEquals(List.of(new LogRecord(0, record("route-b-independent", 31))),
                        firstPoll.get(routeB));

                cluster.stopBroker(2);
                cluster.restartBroker(2);
                Endpoint restartedLeader = cluster.endpoint(2);
                assertNotEquals(originalBroker2, restartedLeader);
                assertEquals(2, cluster.authority().metadata(routeA.topic()).get(0).leaderId());
                assertEquals(1, cluster.authority().metadata(routeA.topic()).get(0).epoch());
                assertEquals(originalBroker2, router.metadata(routeA.topic()).get(0).leader());

                router.refresh(routeA.topic());
                assertEquals(restartedLeader, router.metadata(routeA.topic()).get(0).leader());
                producer.send(routeA.topic(), null, bytes("route-a-after-restart"), 32);
                assertEquals(List.of(new ProduceReceipt(routeA, 1, 2)), producer.flush());
                assertEquals(List.of(record("route-a-before-restart", 30), record("route-a-after-restart", 32)),
                        cluster.partitionLog(2, routeA).read(0, 10, 4_096).stream().map(LogRecord::data).toList());
                assertEquals(List.of(new LogRecord(1, record("route-a-after-restart", 32))),
                        consumer.poll(10, 4_096).get(routeA));

                groupConsumer.subscribe(routeA.topic());
                assertEquals(List.of(new LogRecord(0, record("route-a-before-restart", 30)),
                                new LogRecord(1, record("route-a-after-restart", 32))),
                        groupConsumer.poll(10, 4_096).get(routeA));
                groupConsumer.commitSync();
                Messages.Reply offsetReply = router.controlCall(
                        new Messages.FetchOffsetRequest(new OffsetKey("step27-group", routeA)));
                assertEquals(ErrorCode.NONE, offsetReply.error());
                assertEquals(OptionalLong.of(2), ((Messages.OffsetBody) offsetReply.body()).nextOffset());
            }
        }
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static RecordData record(String value, long timestamp) {
        return new RecordData(null, bytes(value), timestamp);
    }

    @FunctionalInterface
    private interface RequestHandler {
        Messages.Reply handle(BrokerHandler handler, Messages.Request request);
    }

    private static final class TestBroker implements AutoCloseable {
        private static final String TOPIC = "step27-local";
        private final BrokerServer server;
        private final Endpoint endpoint;
        private final PartitionCatalog catalog;
        private final Runnable beforeClose;
        private boolean closed;

        private TestBroker(Path root, RequestHandler dispatch, Runnable beforeClose) {
            AtomicReference<BrokerHandler> handler = new AtomicReference<>();
            this.server = new BrokerServer("127.0.0.1", 0, request -> {
                BrokerHandler current = handler.get();
                return current == null
                        ? Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "broker initialization is incomplete")
                        : dispatch.handle(current, request);
            });
            this.beforeClose = beforeClose;
            Endpoint openedEndpoint = server.start();
            PartitionCatalog openedCatalog = null;
            try {
                openedCatalog = new PartitionCatalog(root, 1, openedEndpoint);
                openedCatalog.createTopic(TOPIC, 1);
                handler.set(new BrokerHandler(openedCatalog, null, null, null));
            } catch (RuntimeException failure) {
                beforeClose.run();
                server.close();
                if (openedCatalog != null) openedCatalog.close();
                throw failure;
            }
            this.endpoint = openedEndpoint;
            this.catalog = openedCatalog;
        }

        private Endpoint endpoint() {
            return endpoint;
        }

        private PartitionCatalog catalog() {
            return catalog;
        }

        @Override
        public void close() {
            if (closed) return;
            closed = true;
            beforeClose.run();
            try {
                server.close();
            } finally {
                catalog.close();
            }
        }
    }
}
