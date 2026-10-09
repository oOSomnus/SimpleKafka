package io.simplekafka.lab;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.ProcessingLoop;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalLong;

/** Reference implementations of the Step 29 cross-layer fault experiments. */
public final class ConsistencyExperiments {
    private static final int RPC_TIMEOUT_MILLIS = 5_000;
    private static final int POLL_RECORDS = 10;
    private static final int POLL_BYTES = 65_536;

    private ConsistencyExperiments() {}

    /**
     * Demonstrates that a successful append with a lost reply is unknown to the ordinary producer.
     */
    public static ConsistencyTrace producerOutcomeUnknown(
            ClusterHarness cluster, TopicPartition tp, RecordData data, FaultProxy proxy) {
        requireInitial(cluster, tp, data, null, null);
        requireProxy(cluster, proxy);

        proxy.dropNextReply(Api.PRODUCE);
        ArrayList<ConsistencyObservation> observations = new ArrayList<>();
        RpcClientFactory clients = routedClients(cluster, proxy);
        try (MetadataRouter router = new MetadataRouter(List.of(cluster.endpoint(2)), clients);
                SimpleConsumer observer =
                        new SimpleConsumer(cluster.client(1, RPC_TIMEOUT_MILLIS), "lab-observer")) {
            observer.assign(List.of(tp));
            try (SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1)) {
                ErrorCode unknown =
                        expectProduceFailure(producer, tp, data, ErrorCode.REQUEST_TIMEOUT);
                if (proxy.forwarded(Api.PRODUCE) != 1
                        || proxy.dropped(Api.PRODUCE) != 1
                        || proxy.armed()) throw invalid("produce reply was not actually dropped");
                requirePhysicalLog(
                        cluster, tp, 1, List.of(new LogRecord(0, data)), "lost-reply append");
                requireEmptyPoll(observer, tp, "lost-reply observation");
                observations.add(
                        LabSupport.observe(
                                "UNKNOWN",
                                unknown,
                                cluster,
                                tp,
                                OptionalLong.of(observer.position(tp)),
                                null,
                                null,
                                Optional.empty()));

                ErrorCode poisoned =
                        expectProduceFailure(producer, tp, data, ErrorCode.REQUEST_TIMEOUT);
                if (proxy.forwarded(Api.PRODUCE) != 1)
                    throw invalid("outcome-unknown producer issued another request");
                requireEmptyPoll(observer, tp, "poisoned-producer observation");
                observations.add(
                        LabSupport.observe(
                                "POISONED",
                                poisoned,
                                cluster,
                                tp,
                                OptionalLong.of(observer.position(tp)),
                                null,
                                null,
                                Optional.empty()));
            }

            AppendResult resend;
            try (SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1)) {
                producer.send(tp.topic(), data.key(), data.value(), data.timestamp());
                List<ProduceReceipt> receipts = producer.flush();
                if (receipts.size() != 1 || !receipts.getFirst().tp().equals(tp))
                    throw invalid("explicit resend did not return one partition receipt");
                ProduceReceipt receipt = receipts.getFirst();
                resend = new AppendResult(receipt.firstOffset(), receipt.nextOffset());
            }
            requirePhysicalLog(
                    cluster,
                    tp,
                    1,
                    List.of(new LogRecord(0, data), new LogRecord(1, data)),
                    "explicit resend");
            if (!resend.equals(new AppendResult(1, 2)))
                throw invalid("explicit resend did not append at offsets [1,2)");
            LabSupport.replicateAndReport(cluster, tp);
            Map<TopicPartition, List<LogRecord>> fetched = observer.poll(POLL_RECORDS, POLL_BYTES);
            requireRecords(
                    fetched.getOrDefault(tp, List.of()),
                    List.of(new LogRecord(0, data), new LogRecord(1, data)),
                    "explicit resend");
            if (proxy.forwarded(Api.PRODUCE) != 2 || proxy.dropped(Api.PRODUCE) != 1)
                throw invalid("proxy did not observe exactly two produce requests");
            observations.add(
                    LabSupport.observe(
                            "EXPLICIT_RESEND",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(observer.position(tp)),
                            null,
                            null,
                            Optional.of(resend)));
        }
        return new ConsistencyTrace(tp, observations);
    }

    /**
     * Shows that a leader acknowledgment does not preserve an uncommitted tail through election.
     */
    public static ConsistencyTrace leaderAckLoss(
            ClusterHarness cluster, TopicPartition tp, RecordData data) {
        requireInitial(cluster, tp, data, null, null);
        AppendResult receipt = produce(cluster, tp, 1, 0, Acks.LEADER, 5_000, data);
        if (!receipt.equals(new AppendResult(0, 1)))
            throw invalid("leader acknowledgment returned an unexpected offset range");
        requirePhysicalLog(
                cluster, tp, 1, List.of(new LogRecord(0, data)), "leader acknowledgment append");

        ArrayList<ConsistencyObservation> observations = new ArrayList<>();
        try (SimpleConsumer reader =
                new SimpleConsumer(cluster.client(1, RPC_TIMEOUT_MILLIS), "lab-leader-ack")) {
            reader.assign(List.of(tp));
            requireEmptyPoll(reader, tp, "leader-ack observation");
            observations.add(
                    LabSupport.observe(
                            "ACKED_NOT_VISIBLE",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(reader.position(tp)),
                            null,
                            null,
                            Optional.of(receipt)));
        }

        cluster.stopBroker(1);
        PartitionMetadata elected = cluster.elect(tp);
        if (elected.leaderId() != 2 || elected.epoch() != 1)
            throw invalid("clean election did not select broker 2 at epoch 1");
        try (SimpleConsumer reader =
                new SimpleConsumer(cluster.client(2, RPC_TIMEOUT_MILLIS), "lab-election")) {
            reader.assign(List.of(tp));
            requireEmptyPoll(reader, tp, "post-election observation");
            observations.add(
                    LabSupport.observe(
                            "ELECTED_WITHOUT_TAIL",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(reader.position(tp)),
                            null,
                            null,
                            Optional.empty()));

            cluster.restartBroker(1);
            long repaired = cluster.reconcile(1, tp);
            if (repaired != 0 || !cluster.admit(1, tp))
                throw invalid("old leader did not reconcile to the elected prefix");
            requireEmptyPoll(reader, tp, "post-reconciliation observation");
            observations.add(
                    LabSupport.observe(
                            "OLD_TAIL_REMOVED",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(reader.position(tp)),
                            null,
                            null,
                            Optional.empty()));
        }
        return new ConsistencyTrace(tp, observations);
    }

    /** Demonstrates a timed-out ALL request becoming visible later without a producer receipt. */
    public static ConsistencyTrace allAckTimeout(
            ClusterHarness cluster, TopicPartition tp, RecordData data) {
        requireInitial(cluster, tp, data, null, null);
        ArrayList<ConsistencyObservation> observations = new ArrayList<>();
        try (SimpleProducer producer =
                new SimpleProducer(cluster.client(1, RPC_TIMEOUT_MILLIS), new Partitioner(), 1)) {
            producer.setAcks(Acks.ALL, 0);
            expectProduceFailure(producer, tp, data, ErrorCode.REQUEST_TIMEOUT);
        }
        requirePhysicalLog(cluster, tp, 1, List.of(new LogRecord(0, data)), "timed-out ALL append");

        try (SimpleConsumer reader =
                new SimpleConsumer(cluster.client(1, RPC_TIMEOUT_MILLIS), "lab-all-timeout")) {
            reader.assign(List.of(tp));
            requireEmptyPoll(reader, tp, "ALL timeout observation");
            observations.add(
                    LabSupport.observe(
                            "TIMEOUT_NOT_VISIBLE",
                            ErrorCode.REQUEST_TIMEOUT,
                            cluster,
                            tp,
                            OptionalLong.of(reader.position(tp)),
                            null,
                            null,
                            Optional.empty()));

            LabSupport.replicateAndReport(cluster, tp);
            Map<TopicPartition, List<LogRecord>> fetched = reader.poll(POLL_RECORDS, POLL_BYTES);
            requireRecords(
                    fetched.getOrDefault(tp, List.of()),
                    List.of(new LogRecord(0, data)),
                    "late ALL visibility");
            observations.add(
                    LabSupport.observe(
                            "LATE_VISIBLE",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(reader.position(tp)),
                            null,
                            null,
                            Optional.empty()));
        }
        return new ConsistencyTrace(tp, observations);
    }

    /**
     * Demonstrates the replay window between a callback side effect and its failed offset commit.
     */
    public static ConsistencyTrace consumerReplay(
            ClusterHarness cluster,
            TopicPartition tp,
            String group,
            RecordData data,
            EffectRecorder effects,
            FaultProxy proxy)
            throws Exception {
        requireInitial(cluster, tp, data, group, effects);
        requireProxy(cluster, proxy);
        AppendResult write = produce(cluster, tp, 1, 0, Acks.LEADER, 5_000, data);
        if (!write.equals(new AppendResult(0, 1)))
            throw invalid("consumer replay setup appended at an unexpected offset");
        requirePhysicalLog(
                cluster, tp, 1, List.of(new LogRecord(0, data)), "consumer replay append");
        LabSupport.replicateAndReport(cluster, tp);

        proxy.rejectNextRequest(Api.COMMIT_OFFSET);
        ArrayList<ConsistencyObservation> observations = new ArrayList<>();
        long failedPosition;
        try (SimpleConsumer consumer =
                new SimpleConsumer(new RpcClient(proxy.endpoint(), RPC_TIMEOUT_MILLIS), group)) {
            consumer.resume(List.of(tp));
            ProcessingLoop loop = new ProcessingLoop(consumer, effects);
            ErrorCode commitFailure;
            try {
                loop.runOnce(POLL_RECORDS, POLL_BYTES);
                throw invalid("the injected commit failure was not observed");
            } catch (CourseException exception) {
                if (exception.code() != ErrorCode.REQUEST_TIMEOUT) throw exception;
                if (!"injected pre-forward failure".equals(exception.getMessage()))
                    throw invalid("proxy returned an unexpected rejection message");
                commitFailure = exception.code();
            }
            failedPosition = consumer.position(tp);
            if (failedPosition != 1
                    || proxy.rejected(Api.COMMIT_OFFSET) != 1
                    || proxy.forwarded(Api.COMMIT_OFFSET) != 0
                    || proxy.armed())
                throw invalid("commit failure did not occur after the callback side effect");
            observations.add(
                    LabSupport.observe(
                            "COMMIT_FAILED",
                            commitFailure,
                            cluster,
                            tp,
                            OptionalLong.of(failedPosition),
                            group,
                            effects,
                            Optional.empty()));
        }

        try (SimpleConsumer consumer =
                new SimpleConsumer(cluster.client(1, RPC_TIMEOUT_MILLIS), group)) {
            consumer.resume(List.of(tp));
            if (consumer.position(tp) != 0)
                throw invalid("failed commit unexpectedly moved the stored consumer offset");
            ProcessingLoop retry = new ProcessingLoop(consumer, effects);
            if (retry.runOnce(POLL_RECORDS, POLL_BYTES) != 1 || consumer.position(tp) != 1)
                throw invalid("fresh consumer did not replay and commit the record");
            observations.add(
                    LabSupport.observe(
                            "REPLAY_COMMITTED",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(consumer.position(tp)),
                            group,
                            effects,
                            Optional.empty()));
        }
        return new ConsistencyTrace(tp, observations);
    }

    /** Demonstrates that group fencing rejects stale requests but cannot undo an old callback. */
    public static ConsistencyTrace staleGroupSideEffect(
            ClusterHarness cluster,
            TopicPartition tp,
            String group,
            RecordData data,
            EffectRecorder effects) {
        requireInitial(cluster, tp, data, group, effects);
        AppendResult write = produce(cluster, tp, 1, 0, Acks.LEADER, 5_000, data);
        if (!write.equals(new AppendResult(0, 1)))
            throw invalid("group fencing setup appended at an unexpected offset");
        requirePhysicalLog(cluster, tp, 1, List.of(new LogRecord(0, data)), "group fencing append");
        LabSupport.replicateAndReport(cluster, tp);

        ArrayList<ConsistencyObservation> observations = new ArrayList<>();
        try (RpcClient client = cluster.client(1, RPC_TIMEOUT_MILLIS)) {
            GroupAssignment first =
                    requireBody(
                                    client.call(
                                            new Messages.JoinGroupRequest(group, "z", tp.topic())),
                                    Messages.GroupBody.class,
                                    "join old member")
                            .assignment();
            requireOwner(first, "z", tp);
            GroupToken oldToken = new GroupToken(group, "z", first.generation());
            Messages.FetchBody oldFetch = fetch(client, tp, 0, oldToken);
            requireRecords(oldFetch.records(), List.of(new LogRecord(0, data)), "old member fetch");
            effects.process(tp, oldFetch.records().getFirst());

            GroupAssignment afterJoin =
                    requireBody(
                                    client.call(
                                            new Messages.JoinGroupRequest(group, "a", tp.topic())),
                                    Messages.GroupBody.class,
                                    "join new member")
                            .assignment();
            requireOwner(afterJoin, "a", tp);
            requireError(
                    client.call(
                            new Messages.CommitOffsetRequest(
                                    new OffsetKey(group, tp), 1, oldToken)),
                    ErrorCode.ILLEGAL_GENERATION,
                    "old member commit");
            requireError(
                    client.call(
                            new Messages.FetchRequest(
                                    tp, 0, 0, POLL_RECORDS, POLL_BYTES, oldToken)),
                    ErrorCode.ILLEGAL_GENERATION,
                    "old member fetch");
            observations.add(
                    LabSupport.observe(
                            "STALE_COMMIT_REJECTED",
                            ErrorCode.ILLEGAL_GENERATION,
                            cluster,
                            tp,
                            OptionalLong.of(oldFetch.records().getFirst().offset() + 1),
                            group,
                            effects,
                            Optional.empty()));

            GroupAssignment current =
                    requireBody(
                                    client.call(new Messages.GroupAssignmentRequest(group, "a")),
                                    Messages.GroupBody.class,
                                    "query current member assignment")
                            .assignment();
            requireOwner(current, "a", tp);
            GroupToken currentToken = new GroupToken(group, "a", current.generation());
            Messages.FetchBody currentFetch = fetch(client, tp, 0, currentToken);
            requireRecords(
                    currentFetch.records(),
                    List.of(new LogRecord(0, data)),
                    "current member fetch");
            effects.process(tp, currentFetch.records().getFirst());
            Messages.Reply committed =
                    client.call(
                            new Messages.CommitOffsetRequest(
                                    new OffsetKey(group, tp), 1, currentToken));
            requireSuccess(committed, "current member commit");
            observations.add(
                    LabSupport.observe(
                            "NEW_OWNER_COMMITTED",
                            ErrorCode.NONE,
                            cluster,
                            tp,
                            OptionalLong.of(currentFetch.records().getFirst().offset() + 1),
                            group,
                            effects,
                            Optional.empty()));
        }
        return new ConsistencyTrace(tp, observations);
    }

    private static void requireInitial(
            ClusterHarness cluster,
            TopicPartition tp,
            RecordData data,
            String group,
            EffectRecorder effects) {
        if (cluster == null || tp == null || data == null)
            throw invalid("cluster, topic-partition, and data are required");
        if (data.producerStamp() != null)
            throw invalid("scenario data must be an ordinary unstamped record");
        try {
            List<PartitionMetadata> topicMetadata = cluster.authority().metadata(tp.topic());
            if (topicMetadata.size() != 1 || !topicMetadata.getFirst().tp().equals(tp))
                throw invalid("experiment requires one partition in the topic");
            var metadata = topicMetadata.getFirst();
            var snapshot = cluster.snapshot(tp);
            if (metadata.leaderId() != 1
                    || metadata.epoch() != 0
                    || !metadata.replicas().equals(ClusterHarness.BROKER_IDS)
                    || !snapshot.isr().equals(ClusterHarness.BROKER_IDS)
                    || snapshot.highWatermark() != 0
                    || !snapshot.perReplicaLEO().equals(Map.of(1, 0L, 2, 0L, 3, 0L))
                    || cluster.authority().partitionState(tp).minISR != 2)
                throw invalid("partition is not in the required empty epoch-0 state");
            for (int brokerId : ClusterHarness.BROKER_IDS) {
                if (!cluster.authority().isOnline(brokerId)
                        || cluster.partitionLog(brokerId, tp).logStartOffset() != 0
                        || cluster.partitionLog(brokerId, tp).logEndOffset() != 0)
                    throw invalid("all replicas must be online with empty logs");
            }
        } catch (CourseException exception) {
            if (exception.code() == ErrorCode.INVALID_REQUEST) throw exception;
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST,
                    "experiment partition is not initialized",
                    exception);
        } catch (RuntimeException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST,
                    "experiment partition is not initialized",
                    exception);
        }
        if ((group == null) != (effects == null))
            throw invalid("consumer group and effect recorder must be supplied together");
        if (effects != null && !effects.snapshot().isEmpty())
            throw invalid("effect recorder must start empty");
        if (group != null) {
            try {
                OffsetKey key = new OffsetKey(group, tp);
                try (RpcClient client = cluster.client(1, RPC_TIMEOUT_MILLIS)) {
                    Messages.OffsetBody body =
                            requireBody(
                                    client.call(new Messages.FetchOffsetRequest(key)),
                                    Messages.OffsetBody.class,
                                    "validate initial committed offset");
                    if (body.nextOffset().isPresent())
                        throw invalid("consumer group must have no committed offset");
                }
            } catch (CourseException exception) {
                if (exception.code() == ErrorCode.INVALID_REQUEST) throw exception;
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST,
                        "consumer group is not in the required state",
                        exception);
            } catch (RuntimeException exception) {
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST,
                        "consumer group is not in the required state",
                        exception);
            }
        }
    }

    private static void requireProxy(ClusterHarness cluster, FaultProxy proxy) {
        if (proxy == null) throw invalid("fault proxy is required");
        try {
            if (!proxy.upstream().equals(cluster.endpoint(1))
                    || proxy.armed()
                    || proxy.endpoint() == null)
                throw invalid("proxy must be started, unarmed, and connected to broker 1");
        } catch (RuntimeException exception) {
            if (exception instanceof CourseException courseException
                    && courseException.code() == ErrorCode.INVALID_REQUEST) throw courseException;
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST,
                    "proxy must be started, unarmed, and connected to broker 1",
                    exception);
        }
    }

    private static RpcClientFactory routedClients(ClusterHarness cluster, FaultProxy proxy) {
        Endpoint leader = cluster.endpoint(1);
        Endpoint faultEndpoint = proxy.endpoint();
        return endpoint ->
                new RpcClient(
                        endpoint.equals(leader) ? faultEndpoint : endpoint, RPC_TIMEOUT_MILLIS);
    }

    private static ErrorCode expectProduceFailure(
            SimpleProducer producer, TopicPartition tp, RecordData data, ErrorCode expected) {
        try {
            producer.send(tp.topic(), data.key(), data.value(), data.timestamp());
        } catch (CourseException exception) {
            if (exception.code() == expected) return exception.code();
            throw exception;
        }
        throw invalid("producer request unexpectedly succeeded instead of " + expected);
    }

    private static AppendResult produce(
            ClusterHarness cluster,
            TopicPartition tp,
            int brokerId,
            int epoch,
            Acks acks,
            long timeoutMillis,
            RecordData data) {
        try (RpcClient client = cluster.client(brokerId, RPC_TIMEOUT_MILLIS)) {
            Messages.ProduceBody body =
                    requireBody(
                            client.call(
                                    new Messages.ProduceRequest(
                                            tp, epoch, acks, timeoutMillis, List.of(data))),
                            Messages.ProduceBody.class,
                            "produce scenario record");
            return body.result();
        }
    }

    private static void requireEmptyPoll(SimpleConsumer consumer, TopicPartition tp, String phase) {
        Map<TopicPartition, List<LogRecord>> fetched = consumer.poll(POLL_RECORDS, POLL_BYTES);
        if (!fetched.getOrDefault(tp, List.of()).isEmpty())
            throw invalid(phase + " unexpectedly exposed records");
    }

    private static void requireRecords(
            List<LogRecord> actual, List<LogRecord> expected, String phase) {
        if (!actual.equals(expected)) throw invalid(phase + " returned unexpected records");
    }

    private static void requirePhysicalLog(
            ClusterHarness cluster,
            TopicPartition tp,
            int brokerId,
            List<LogRecord> expected,
            String phase) {
        List<LogRecord> records = cluster.partitionLog(brokerId, tp).read(0, 10, POLL_BYTES);
        if (!records.equals(expected))
            throw invalid(phase + " did not persist the supplied record data");
    }

    private static Messages.FetchBody fetch(
            RpcClient client, TopicPartition tp, long offset, GroupToken token) {
        PartitionMetadata metadata =
                requireBody(
                                client.call(new Messages.MetadataRequest(tp.topic())),
                                Messages.MetadataBody.class,
                                "fetch metadata")
                        .partitions()
                        .stream()
                        .filter(partition -> partition.tp().equals(tp))
                        .findFirst()
                        .orElseThrow(() -> invalid("topic-partition is missing from metadata"));
        return requireBody(
                client.call(
                        new Messages.FetchRequest(
                                tp, metadata.epoch(), offset, POLL_RECORDS, POLL_BYTES, token)),
                Messages.FetchBody.class,
                "fetch records");
    }

    private static void requireOwner(GroupAssignment assignment, String member, TopicPartition tp) {
        if (!assignment.assignments().getOrDefault(member, List.of()).contains(tp))
            throw invalid("group member does not own the experiment partition");
    }

    private static void requireSuccess(Messages.Reply reply, String operation) {
        if (reply.error() != ErrorCode.NONE)
            throw new CourseException(reply.error(), operation + " failed: " + reply.body());
    }

    private static void requireError(Messages.Reply reply, ErrorCode expected, String operation) {
        if (reply.error() != expected)
            throw invalid(operation + " returned " + reply.error() + " instead of " + expected);
    }

    private static <T extends Messages.Response> T requireBody(
            Messages.Reply reply, Class<T> bodyType, String operation) {
        requireSuccess(reply, operation);
        if (!bodyType.isInstance(reply.body()))
            throw invalid(operation + " returned an unexpected response body");
        return bodyType.cast(reply.body());
    }

    private static CourseException invalid(String message) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message);
    }
}
