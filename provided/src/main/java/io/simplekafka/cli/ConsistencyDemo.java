package io.simplekafka.cli;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.client.DurableEffectStore;
import io.simplekafka.client.IdempotentProducer;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.client.ProcessingLoop;
import io.simplekafka.client.RecordProcessor;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.lab.ConsistencyExperiments;
import io.simplekafka.lab.ConsistencyObservation;
import io.simplekafka.lab.ConsistencyTrace;
import io.simplekafka.lab.EffectRecorder;
import io.simplekafka.lab.FaultProxy;
import io.simplekafka.lab.LabSupport;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.BusinessEvent;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import io.simplekafka.protocol.ProducerBatchFingerprint;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;
import java.util.stream.Stream;

/** Runs real TCP and on-disk examples for producer, broker, and consumer consistency boundaries. */
public final class ConsistencyDemo {
    private static final int RPC_TIMEOUT_MILLIS = 5_000;
    private static final int PRODUCE_TIMEOUT_MILLIS = 8_000;
    private static final int MAX_RECORDS = 10;
    private static final TimeSource FIXED_CLOCK = () -> 1_000L;

    private ConsistencyDemo() {}

    /** Runs all scenarios, or one of {@code faults}, {@code producer}, and {@code consumer}. */
    public static void main(String[] args) throws Exception {
        String selected = args.length == 0 ? "all" : args[0];
        if (args.length > 1
                || !(selected.equals("all")
                        || selected.equals("faults")
                        || selected.equals("producer")
                        || selected.equals("consumer"))) {
            System.err.println("usage: ConsistencyDemo [all|faults|producer|consumer]");
            throw new IllegalArgumentException("unknown consistency demo selection");
        }
        try {
            if (selected.equals("all") || selected.equals("faults")) runFaultScenarios();
            if (selected.equals("all") || selected.equals("producer")) runProducerScenario();
            if (selected.equals("all") || selected.equals("consumer")) runConsumerScenario();
            System.out.println("CONSISTENCY SCENARIO PASS");
        } catch (Exception failure) {
            System.err.println("CONSISTENCY SCENARIO FAIL: " + safeMessage(failure));
            throw failure;
        } catch (Error failure) {
            System.err.println("CONSISTENCY SCENARIO FAIL: " + safeMessage(failure));
            throw failure;
        }
    }

    private static void runFaultScenarios() throws Exception {
        withTemporaryRoot("fault-producer", ConsistencyDemo::runUnknownProducerScenario);
        withTemporaryRoot("fault-leader", ConsistencyDemo::runLeaderAckScenario);
        withTemporaryRoot("fault-all", ConsistencyDemo::runAllAckScenario);
        withTemporaryRoot("fault-consumer-replay", ConsistencyDemo::runConsumerReplayScenario);
        withTemporaryRoot("fault-group-fence", ConsistencyDemo::runStaleGroupScenario);
        System.out.println("FAULTS PASS scenarios=5");
    }

    private static void runUnknownProducerScenario(Path root) {
        TopicPartition tp = new TopicPartition("demo-unknown", 0);
        RecordData data = record("customer-A", "request-unknown-73", 1_723);
        try (ClusterHarness cluster = openCluster(root, tp);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            proxy.start();
            ConsistencyTrace trace =
                    ConsistencyExperiments.producerOutcomeUnknown(cluster, tp, data, proxy);
            requireTrace(trace, tp, List.of("UNKNOWN", "POISONED", "EXPLICIT_RESEND"));
            requireObservation(
                    trace.observations().get(0),
                    "UNKNOWN",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    0,
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            requireObservation(
                    trace.observations().get(1),
                    "POISONED",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    0,
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            List<LogRecord> duplicates = List.of(new LogRecord(0, data), new LogRecord(1, data));
            requireObservation(
                    trace.observations().get(2),
                    "EXPLICIT_RESEND",
                    ErrorCode.NONE,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 2L, 2, 2L, 3, 2L),
                    2,
                    OptionalLong.of(2),
                    OptionalLong.empty(),
                    duplicates,
                    List.of(),
                    Optional.of(new AppendResult(1, 2)));
            requireLogContents(cluster, tp, duplicates);
            requireProxyCounts(proxy, Api.PRODUCE, 2, 1, 0);
        }
    }

    private static void runLeaderAckScenario(Path root) {
        TopicPartition tp = new TopicPartition("demo-leader-ack", 0);
        RecordData data = record("leader-tail", "acked-but-uncommitted", 2_419);
        try (ClusterHarness cluster = openCluster(root, tp)) {
            ConsistencyTrace trace = ConsistencyExperiments.leaderAckLoss(cluster, tp, data);
            requireTrace(
                    trace,
                    tp,
                    List.of("ACKED_NOT_VISIBLE", "ELECTED_WITHOUT_TAIL", "OLD_TAIL_REMOVED"));
            requireObservation(
                    trace.observations().get(0),
                    "ACKED_NOT_VISIBLE",
                    ErrorCode.NONE,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    0,
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.of(new AppendResult(0, 1)));
            requireObservation(
                    trace.observations().get(1),
                    "ELECTED_WITHOUT_TAIL",
                    ErrorCode.NONE,
                    2,
                    1,
                    List.of(2),
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    0,
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            requireObservation(
                    trace.observations().get(2),
                    "OLD_TAIL_REMOVED",
                    ErrorCode.NONE,
                    2,
                    1,
                    List.of(1, 2),
                    Map.of(1, 0L, 2, 0L, 3, 0L),
                    0,
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            requireLogContents(cluster, tp, List.of());
        }
    }

    private static void runAllAckScenario(Path root) {
        TopicPartition tp = new TopicPartition("demo-all-timeout", 0);
        RecordData data = record("all-timeout", "visible-after-timeout", 2_981);
        try (ClusterHarness cluster = openCluster(root, tp)) {
            ConsistencyTrace trace = ConsistencyExperiments.allAckTimeout(cluster, tp, data);
            requireTrace(trace, tp, List.of("TIMEOUT_NOT_VISIBLE", "LATE_VISIBLE"));
            requireObservation(
                    trace.observations().get(0),
                    "TIMEOUT_NOT_VISIBLE",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 0L, 3, 0L),
                    0,
                    OptionalLong.of(0),
                    OptionalLong.empty(),
                    List.of(),
                    List.of(),
                    Optional.empty());
            List<LogRecord> visible = List.of(new LogRecord(0, data));
            requireObservation(
                    trace.observations().get(1),
                    "LATE_VISIBLE",
                    ErrorCode.NONE,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    1,
                    OptionalLong.of(1),
                    OptionalLong.empty(),
                    visible,
                    List.of(),
                    Optional.empty());
            requireLogContents(cluster, tp, visible);
        }
    }

    private static void runConsumerReplayScenario(Path root) throws Exception {
        TopicPartition tp = new TopicPartition("demo-consumer-replay", 0);
        String group = "demo-replay-group";
        RecordData data = record("event-A", "side-effect-before-commit", 3_141);
        EffectRecorder effects = new EffectRecorder();
        try (ClusterHarness cluster = openCluster(root, tp);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            proxy.start();
            ConsistencyTrace trace =
                    ConsistencyExperiments.consumerReplay(cluster, tp, group, data, effects, proxy);
            requireTrace(trace, tp, List.of("COMMIT_FAILED", "REPLAY_COMMITTED"));
            List<LogRecord> delivered = List.of(new LogRecord(0, data), new LogRecord(0, data));
            requireObservation(
                    trace.observations().get(0),
                    "COMMIT_FAILED",
                    ErrorCode.REQUEST_TIMEOUT,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    1,
                    OptionalLong.of(1),
                    OptionalLong.empty(),
                    List.of(new LogRecord(0, data)),
                    List.of(new LogRecord(0, data)),
                    Optional.empty());
            requireObservation(
                    trace.observations().get(1),
                    "REPLAY_COMMITTED",
                    ErrorCode.NONE,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    1,
                    OptionalLong.of(1),
                    OptionalLong.of(1),
                    List.of(new LogRecord(0, data)),
                    delivered,
                    Optional.empty());
            if (!effects.snapshot().equals(delivered)
                    || committedOffset(cluster, group, tp).orElse(-1) != 1)
                throw new IllegalStateException(
                        "consumer replay did not preserve real callback/offset state");
            requireProxyCounts(proxy, Api.COMMIT_OFFSET, 0, 0, 1);
            requireLogContents(cluster, tp, List.of(new LogRecord(0, data)));
        }
    }

    private static void runStaleGroupScenario(Path root) {
        TopicPartition tp = new TopicPartition("demo-stale-group", 0);
        String group = "demo-stale-group-group";
        RecordData data = record("event-old-owner", "fenced-but-not-undone", 3_579);
        EffectRecorder effects = new EffectRecorder();
        try (ClusterHarness cluster = openCluster(root, tp)) {
            ConsistencyTrace trace =
                    ConsistencyExperiments.staleGroupSideEffect(cluster, tp, group, data, effects);
            requireTrace(trace, tp, List.of("STALE_COMMIT_REJECTED", "NEW_OWNER_COMMITTED"));
            List<LogRecord> duplicated = List.of(new LogRecord(0, data), new LogRecord(0, data));
            requireObservation(
                    trace.observations().get(0),
                    "STALE_COMMIT_REJECTED",
                    ErrorCode.ILLEGAL_GENERATION,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    1,
                    OptionalLong.of(1),
                    OptionalLong.empty(),
                    List.of(new LogRecord(0, data)),
                    List.of(new LogRecord(0, data)),
                    Optional.empty());
            requireObservation(
                    trace.observations().get(1),
                    "NEW_OWNER_COMMITTED",
                    ErrorCode.NONE,
                    1,
                    0,
                    ClusterHarness.BROKER_IDS,
                    Map.of(1, 1L, 2, 1L, 3, 1L),
                    1,
                    OptionalLong.of(1),
                    OptionalLong.of(1),
                    List.of(new LogRecord(0, data)),
                    duplicated,
                    Optional.empty());
            if (!effects.snapshot().equals(duplicated)
                    || committedOffset(cluster, group, tp).orElse(-1) != 1)
                throw new IllegalStateException(
                        "group fencing changed the observed callback or commit state");
            requireLogContents(cluster, tp, List.of(new LogRecord(0, data)));
        }
    }

    private static void runProducerScenario() throws Exception {
        withTemporaryRoot(
                "idempotent-producer",
                root -> {
                    TopicPartition tp = new TopicPartition("demo-idempotent", 0);
                    List<RecordData> batch =
                            List.of(
                                    record("key-A", "batch-value-A", 4_101),
                                    record("key-B", "batch-value-B", 4_102),
                                    record("key-C", "batch-value-C", 4_103));
                    try (ClusterHarness cluster = openCluster(root, tp);
                            FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
                        proxy.start();
                        Endpoint initialLeader = cluster.endpoint(1);
                        List<Endpoint> bootstrap = List.of(cluster.endpoint(3));
                        RpcClientFactory factory =
                                endpoint ->
                                        new RpcClient(
                                                endpoint.equals(initialLeader)
                                                        ? proxy.endpoint()
                                                        : endpoint,
                                                RPC_TIMEOUT_MILLIS);
                        try (MetadataRouter router = new MetadataRouter(bootstrap, factory);
                                IdempotentProducer producer =
                                        new IdempotentProducer(router, 7, 0)) {
                            proxy.dropNextReply(Api.IDEMPOTENT_PRODUCE);
                            CourseException unknown =
                                    expectFailure(
                                            () ->
                                                    producer.send(
                                                            tp,
                                                            batch,
                                                            Acks.LEADER,
                                                            PRODUCE_TIMEOUT_MILLIS),
                                            ErrorCode.REQUEST_TIMEOUT);
                            if (unknown.code() != ErrorCode.REQUEST_TIMEOUT
                                    || proxy.forwarded(Api.IDEMPOTENT_PRODUCE) != 1
                                    || proxy.dropped(Api.IDEMPOTENT_PRODUCE) != 1
                                    || proxy.armed())
                                throw new IllegalStateException(
                                        "idempotent producer response was not lost after append");
                            requireBrokerLogContents(
                                    cluster,
                                    1,
                                    tp,
                                    stampedRecords(
                                            7,
                                            0,
                                            0,
                                            batch,
                                            ProducerBatchFingerprint.fingerprint(batch)));

                            LabSupport.replicateAndReport(cluster, tp);
                            requireAllReplicaLeo(cluster, tp, 3, 3);
                            cluster.stopBroker(1);
                            PartitionMetadata elected = cluster.elect(tp);
                            if (elected.leaderId() != 2 || elected.epoch() != 1)
                                throw new IllegalStateException(
                                        "clean election did not select broker 2 at epoch 1");

                            ProduceReceipt retry = producer.retry(tp);
                            if (!retry.equals(new ProduceReceipt(tp, 0, 3)))
                                throw new IllegalStateException(
                                        "explicit retry did not return the original [0,3) range");
                            requireAllReplicaLeo(cluster, tp, 3, 3);
                            List<LogRecord> expected =
                                    stampedRecords(
                                            7,
                                            0,
                                            0,
                                            batch,
                                            ProducerBatchFingerprint.fingerprint(batch));
                            requireLogContents(cluster, tp, expected);

                            PartitionLog oldLog = cluster.partitionLog(2, tp);
                            cluster.reopenBrokerFromDisk(2);
                            if (oldLog == cluster.partitionLog(2, tp))
                                throw new IllegalStateException(
                                        "broker reopen reused its old partition log object");
                            if (!cluster.replicaState(2, tp).isOnline())
                                throw new IllegalStateException(
                                        "reopened leader did not return online");
                            try (RpcClient client = cluster.client(2, RPC_TIMEOUT_MILLIS)) {
                                Messages.Reply duplicate =
                                        client.call(
                                                new Messages.IdempotentProduceRequest(
                                                        tp,
                                                        elected.epoch(),
                                                        Acks.LEADER,
                                                        PRODUCE_TIMEOUT_MILLIS,
                                                        7,
                                                        0,
                                                        0,
                                                        batch));
                                Messages.ProduceBody body =
                                        requireBody(
                                                duplicate,
                                                Messages.ProduceBody.class,
                                                "disk-reopened duplicate");
                                if (!body.result().equals(new AppendResult(0, 3)))
                                    throw new IllegalStateException(
                                            "disk-recovered producer state lost the old range");

                                Messages.FetchBody fetched =
                                        requireBody(
                                                client.call(
                                                        new Messages.FetchRequest(
                                                                tp,
                                                                elected.epoch(),
                                                                0,
                                                                MAX_RECORDS,
                                                                MessageCodec.MAX_FETCH_BYTE_BUDGET,
                                                                null)),
                                                Messages.FetchBody.class,
                                                "fetch after disk reopen");
                                if (!fetched.records().equals(expected)
                                        || fetched.logEndOffset() != 3
                                        || fetched.highWatermark() != 3)
                                    throw new IllegalStateException(
                                            "disk-reopened fetch did not retain one stamped batch");
                            }
                            requireAllReplicaLeo(cluster, tp, 3, 3);
                            requireLogContents(cluster, tp, expected);
                        }
                    }
                });
        System.out.println(
                "IDEMPOTENT_PRODUCER PASS first=[0,3) retry=[0,3) leader=2 epoch=1 records=3");
    }

    private static void runConsumerScenario() throws Exception {
        withTemporaryRoot(
                "idempotent-consumer",
                root -> {
                    TopicPartition tp = new TopicPartition("demo-effects", 0);
                    String group = "demo-effects-group";
                    BusinessEvent first = new BusinessEvent("business-A", "account", 10);
                    BusinessEvent duplicateAtAnotherOffset =
                            new BusinessEvent("business-A", "account", 10);
                    BusinessEvent second = new BusinessEvent("business-B", "account", -3);
                    List<BusinessEvent> events = List.of(first, duplicateAtAnotherOffset, second);
                    List<RecordData> records =
                            events.stream()
                                    .map(
                                            event ->
                                                    new RecordData(
                                                            null,
                                                            MessageCodec.encodeBusinessEvent(event),
                                                            0))
                                    .toList();
                    Path ledgerDirectory = root.resolve("business-ledger");
                    ArrayList<Long> callbackOffsets = new ArrayList<>();

                    try (ClusterHarness cluster = openCluster(root.resolve("cluster"), tp);
                            FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
                        AppendResult appended = produce(cluster, tp, records);
                        if (!appended.equals(new AppendResult(0, 3)))
                            throw new IllegalStateException(
                                    "business event setup did not append [0,3)");
                        LabSupport.replicateAndReport(cluster, tp);
                        requireAllReplicaLeo(cluster, tp, 3, 3);
                        proxy.start();
                        proxy.rejectNextRequest(Api.COMMIT_OFFSET);

                        try (DurableEffectStore store = new DurableEffectStore(ledgerDirectory);
                                SimpleConsumer consumer =
                                        new SimpleConsumer(
                                                new RpcClient(proxy.endpoint(), RPC_TIMEOUT_MILLIS),
                                                group)) {
                            consumer.resume(List.of(tp));
                            if (consumer.position(tp) != 0)
                                throw new IllegalStateException(
                                        "consumer did not resume from offset zero");
                            ProcessingLoop loop =
                                    new ProcessingLoop(consumer, processor(store, callbackOffsets));
                            CourseException failure =
                                    expectFailure(
                                            () ->
                                                    loop.runOnce(
                                                            MAX_RECORDS,
                                                            MessageCodec.MAX_FETCH_BYTE_BUDGET),
                                            ErrorCode.REQUEST_TIMEOUT);
                            if (!"injected pre-forward failure".equals(failure.getMessage())
                                    || consumer.position(tp) != 3
                                    || !callbackOffsets.equals(List.of(0L, 1L, 2L))
                                    || store.balance("account") != 7
                                    || proxy.rejected(Api.COMMIT_OFFSET) != 1
                                    || proxy.forwarded(Api.COMMIT_OFFSET) != 0
                                    || proxy.armed())
                                throw new IllegalStateException(
                                        "commit rejection did not follow all business callbacks");
                            if (committedOffset(cluster, group, tp).isPresent())
                                throw new IllegalStateException(
                                        "failed commit changed the durable consumer offset");
                        }
                        requireLedger(ledgerDirectory, List.of(first, second));

                        try (DurableEffectStore reopened = new DurableEffectStore(ledgerDirectory);
                                SimpleConsumer consumer =
                                        new SimpleConsumer(
                                                cluster.client(1, RPC_TIMEOUT_MILLIS), group)) {
                            consumer.resume(List.of(tp));
                            if (consumer.position(tp) != 0)
                                throw new IllegalStateException(
                                        "fresh consumer did not replay from committed offset zero");
                            ProcessingLoop retry =
                                    new ProcessingLoop(
                                            consumer, processor(reopened, callbackOffsets));
                            if (retry.runOnce(MAX_RECORDS, MessageCodec.MAX_FETCH_BYTE_BUDGET) != 3
                                    || consumer.position(tp) != 3)
                                throw new IllegalStateException(
                                        "replayed consumer did not process and commit all records");
                            if (reopened.balance("account") != 7)
                                throw new IllegalStateException(
                                        "replayed business events changed the durable balance");
                        }
                        requireLedger(ledgerDirectory, List.of(first, second));

                        OptionalLong committed = committedOffset(cluster, group, tp);
                        if (!callbackOffsets.equals(List.of(0L, 1L, 2L, 0L, 1L, 2L))
                                || !committed.equals(OptionalLong.of(3))
                                || proxy.rejected(Api.COMMIT_OFFSET) != 1
                                || proxy.forwarded(Api.COMMIT_OFFSET) != 0)
                            throw new IllegalStateException(
                                    "consumer replay did not preserve expected callback/commit evidence");
                    }
                });
        System.out.println(
                "IDEMPOTENT_EFFECTS PASS callbacks=6 ledgerEntries=2 balance=7 committedOffset=3");
    }

    private static RecordProcessor processor(DurableEffectStore store, List<Long> callbackOffsets) {
        return (tp, record) -> {
            callbackOffsets.add(record.offset());
            store.apply(MessageCodec.decodeBusinessEvent(record.data().value()));
        };
    }

    private static ClusterHarness openCluster(Path root, TopicPartition tp) {
        ClusterHarness cluster = new ClusterHarness(root, FIXED_CLOCK);
        try {
            cluster.start();
            cluster.createTopic(tp.topic(), 1, 2);
            if (!cluster.snapshot(tp).isr().equals(ClusterHarness.BROKER_IDS)
                    || cluster.snapshot(tp).highWatermark() != 0)
                throw new IllegalStateException(
                        "demo cluster did not start with an empty RF3 partition");
            return cluster;
        } catch (RuntimeException failure) {
            try {
                cluster.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    private static RecordData record(String key, String value, long timestamp) {
        return new RecordData(
                key.getBytes(StandardCharsets.UTF_8),
                value.getBytes(StandardCharsets.UTF_8),
                timestamp);
    }

    private static List<LogRecord> stampedRecords(
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records,
            byte[] hash) {
        ArrayList<LogRecord> result = new ArrayList<>(records.size());
        for (int index = 0; index < records.size(); index++) {
            RecordData data = records.get(index);
            ProducerStamp stamp =
                    new ProducerStamp(
                            producerId, producerEpoch, firstSequence, records.size(), index, hash);
            result.add(
                    new LogRecord(
                            index,
                            new RecordData(data.key(), data.value(), data.timestamp(), stamp)));
        }
        return List.copyOf(result);
    }

    private static AppendResult produce(
            ClusterHarness cluster, TopicPartition tp, List<RecordData> records) {
        try (RpcClient client = cluster.client(1, RPC_TIMEOUT_MILLIS)) {
            Messages.ProduceBody body =
                    requireBody(
                            client.call(
                                    new Messages.ProduceRequest(
                                            tp, 0, Acks.LEADER, PRODUCE_TIMEOUT_MILLIS, records)),
                            Messages.ProduceBody.class,
                            "produce business events");
            return body.result();
        }
    }

    private static OptionalLong committedOffset(
            ClusterHarness cluster, String group, TopicPartition tp) {
        try (RpcClient client = cluster.client(1, RPC_TIMEOUT_MILLIS)) {
            Messages.OffsetBody body =
                    requireBody(
                            client.call(new Messages.FetchOffsetRequest(new OffsetKey(group, tp))),
                            Messages.OffsetBody.class,
                            "fetch committed offset");
            return body.nextOffset();
        }
    }

    private static void requireLedger(Path directory, List<BusinessEvent> expected) {
        try (PartitionLog log = new PartitionLog(directory, 1_048_576, 1)) {
            if (log.logStartOffset() != 0 || log.logEndOffset() != expected.size())
                throw new IllegalStateException(
                        "durable ledger range differs from expected event count");
            List<BusinessEvent> actual =
                    log.read(0, MAX_RECORDS, MessageCodec.MAX_FETCH_BYTE_BUDGET).stream()
                            .map(
                                    record -> {
                                        if (record.data().key() != null
                                                || record.data().timestamp() != 0
                                                || record.data().producerStamp() != null)
                                            throw new IllegalStateException(
                                                    "durable ledger record metadata is invalid");
                                        return MessageCodec.decodeBusinessEvent(
                                                record.data().value());
                                    })
                            .toList();
            if (!actual.equals(expected))
                throw new IllegalStateException(
                        "durable ledger contents differ from expected events");
        }
    }

    private static void requireLogContents(
            ClusterHarness cluster, TopicPartition tp, List<LogRecord> expected) {
        for (int brokerId : ClusterHarness.BROKER_IDS)
            requireBrokerLogContents(cluster, brokerId, tp, expected);
    }

    private static void requireBrokerLogContents(
            ClusterHarness cluster, int brokerId, TopicPartition tp, List<LogRecord> expected) {
        PartitionLog log = cluster.partitionLog(brokerId, tp);
        List<LogRecord> actual =
                log.read(log.logStartOffset(), MAX_RECORDS, MessageCodec.MAX_FETCH_BYTE_BUDGET);
        if (!actual.equals(expected))
            throw new IllegalStateException(
                    "broker " + brokerId + " log differs from expected records: " + actual);
    }

    private static void requireAllReplicaLeo(
            ClusterHarness cluster, TopicPartition tp, long expectedLeo, long expectedHw) {
        for (int brokerId : ClusterHarness.BROKER_IDS) {
            if (cluster.partitionLog(brokerId, tp).logEndOffset() != expectedLeo)
                throw new IllegalStateException(
                        "broker " + brokerId + " has an unexpected physical LEO");
        }
        if (cluster.snapshot(tp).highWatermark() != expectedHw)
            throw new IllegalStateException("partition has an unexpected high watermark");
    }

    private static void requireProxyCounts(
            FaultProxy proxy, short api, int forwarded, int dropped, int rejected) {
        if (proxy.forwarded(api) != forwarded
                || proxy.dropped(api) != dropped
                || proxy.rejected(api) != rejected
                || proxy.armed())
            throw new IllegalStateException("fault proxy did not perform the expected real action");
    }

    private static void requireTrace(
            ConsistencyTrace trace, TopicPartition tp, List<String> expectedPhases) {
        if (!trace.tp().equals(tp)
                || !trace.observations().stream()
                        .map(ConsistencyObservation::phase)
                        .toList()
                        .equals(expectedPhases))
            throw new IllegalStateException(
                    "consistency experiment returned unexpected observations");
    }

    private static void requireObservation(
            ConsistencyObservation observation,
            String phase,
            ErrorCode error,
            int leaderId,
            int epoch,
            List<Integer> isr,
            Map<Integer, Long> leo,
            long highWatermark,
            OptionalLong position,
            OptionalLong committedOffset,
            List<LogRecord> visible,
            List<LogRecord> effects,
            Optional<AppendResult> receipt) {
        requireObservation(
                observation,
                phase,
                error,
                leaderId,
                epoch,
                isr,
                leo,
                leo,
                highWatermark,
                position,
                committedOffset,
                visible,
                effects,
                receipt);
    }

    private static void requireObservation(
            ConsistencyObservation observation,
            String phase,
            ErrorCode error,
            int leaderId,
            int epoch,
            List<Integer> isr,
            Map<Integer, Long> physicalLeo,
            Map<Integer, Long> reportedLeo,
            long highWatermark,
            OptionalLong position,
            OptionalLong committedOffset,
            List<LogRecord> visible,
            List<LogRecord> effects,
            Optional<AppendResult> receipt) {
        if (!observation.phase().equals(phase)
                || !observation.replication().isr().equals(isr)
                || !observation.replication().replicas().equals(ClusterHarness.BROKER_IDS)
                || observation.replication().leaderId() != leaderId
                || observation.replication().epoch() != epoch
                || observation.replication().highWatermark() != highWatermark
                || !observation.physicalLeo().equals(physicalLeo)
                || !observation.replication().perReplicaLEO().equals(reportedLeo)
                || !observation.position().equals(position)
                || !observation.committedOffset().equals(committedOffset)
                || !observation.visible().equals(visible)
                || !observation.effects().equals(effects)
                || !observation.receipt().equals(receipt))
            throw new IllegalStateException(
                    "observation " + phase + " differs from expected real state");
    }

    private static <T extends Messages.Response> T requireBody(
            Messages.Reply reply, Class<T> bodyType, String operation) {
        if (reply.error() != ErrorCode.NONE)
            throw new CourseException(reply.error(), operation + " failed: " + reply.body());
        if (!bodyType.isInstance(reply.body()))
            throw new IllegalStateException(operation + " returned an unexpected response body");
        return bodyType.cast(reply.body());
    }

    private static CourseException expectFailure(CheckedAction action, ErrorCode expected)
            throws Exception {
        try {
            action.run();
        } catch (CourseException failure) {
            if (failure.code() == expected) return failure;
            throw failure;
        }
        throw new IllegalStateException("operation succeeded instead of failing with " + expected);
    }

    private static void withTemporaryRoot(String label, RootAction action) throws Exception {
        Path root = Files.createTempDirectory("simple-kafka-consistency-" + label + "-");
        try {
            action.run(root);
        } finally {
            deleteTree(root);
        }
    }

    private static void deleteTree(Path root) throws IOException {
        if (!Files.exists(root)) return;
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted((left, right) -> right.compareTo(left)).toList())
                Files.deleteIfExists(path);
        }
    }

    private static String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }

    @FunctionalInterface
    private interface RootAction {
        void run(Path root) throws Exception;
    }

    @FunctionalInterface
    private interface CheckedAction {
        void run() throws Exception;
    }
}
