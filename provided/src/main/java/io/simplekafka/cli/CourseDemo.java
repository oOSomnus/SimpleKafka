package io.simplekafka.cli;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.client.GroupConsumer;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.client.Partitioner;
import io.simplekafka.client.SimpleProducer;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.Acks;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.SystemTimeSource;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.ReentrantLock;

/** Runs the complete replication/failover scenario against three real TCP broker sockets. */
public final class CourseDemo {
    private static final String TOPIC = "orders";
    private static final String GROUP = "workers";
    private static final int RPC_TIMEOUT_MILLIS = 5_000;
    private static final int PRODUCE_TIMEOUT_MILLIS = 8_000;

    private CourseDemo() {}

    /**
     * Runs the replication and failover scenario in a temporary directory.
     *
     * <p>Accepts no arguments or the single argument {@code scenario}. The run prints
     * {@code SCENARIO PASS} on success or {@code SCENARIO FAIL: ...} before rethrowing a failure.
     * It creates a temporary {@code simple-kafka-course-demo-*} directory and attempts to delete
     * it before returning; a deletion failure is reported to standard error.
     *
     * @param args zero arguments or {@code ["scenario"]}
     * @throws IllegalArgumentException if the arguments do not match the accepted form
     * @throws RuntimeException if the scenario fails; checked I/O failures are wrapped in
     *     {@link IllegalStateException}
     */
    public static void main(String[] args) {
        if (args.length > 1 || args.length == 1 && !args[0].equals("scenario")) {
            System.err.println("usage: CourseDemo [scenario]");
            throw new IllegalArgumentException("unknown demo scenario");
        }
        Path root = null;
        try {
            root = Files.createTempDirectory("simple-kafka-course-demo-");
            runScenario(root);
        } catch (RuntimeException | IOException failure) {
            System.err.println("SCENARIO FAIL: " + safeMessage(failure));
            throw failure instanceof RuntimeException runtime
                    ? runtime
                    : new IllegalStateException(failure);
        } finally {
            if (root != null) deleteTree(root);
        }
    }

    private static void runScenario(Path root) {
        try (ClusterHarness harness = new ClusterHarness(root, SystemTimeSource.INSTANCE)) {
            harness.start();
            harness.createTopic(TOPIC, 2, 2);
            printMetadata(harness, "INITIAL");

            List<Endpoint> bootstrap =
                    ClusterHarness.BROKER_IDS.stream().map(harness::endpoint).toList();
            RpcClientFactory factory = endpoint -> new RpcClient(endpoint, RPC_TIMEOUT_MILLIS);
            try (MetadataRouter router = new MetadataRouter(bootstrap, factory);
                    SimpleProducer producer = new SimpleProducer(router, new Partitioner(), 1);
                    GroupConsumer c1 = new GroupConsumer(router, GROUP, "c1");
                    GroupConsumer c2 = new GroupConsumer(router, GROUP, "c2");
                    ReplicationPump pump = new ReplicationPump(harness)) {
                producer.setAcks(Acks.ALL, PRODUCE_TIMEOUT_MILLIS);
                pump.start();
                for (int index = 0; index < 6; index++) {
                    int partition = index % 2;
                    byte[] key = keyForPartition(partition, 2).getBytes(StandardCharsets.UTF_8);
                    String value = "order-" + index;
                    producer.send(
                            TOPIC,
                            key,
                            value.getBytes(StandardCharsets.UTF_8),
                            System.currentTimeMillis());
                }
                List<ProduceReceipt> initialReceipts = producer.flush();
                System.out.println(
                        "PRODUCED initial=6 receipts=" + receiptSummary(initialReceipts));

                c1.subscribe(TOPIC);
                c2.subscribe(TOPIC);
                assertOldGenerationCommitRejected(
                        harness.endpoint(2), new TopicPartition(TOPIC, 0));
                printAssignment(harness.endpoint(2), "c1", "c2");
                Set<String> consumed = new HashSet<>();
                consumeUntil(c1, c2, consumed, 6);
                if (consumed.size() != 6)
                    throw new IllegalStateException(
                            "initial group processing did not consume six records");
                printOffsets(harness.endpoint(2), "initial", Map.of(0, 3L, 1, 3L));

                TopicPartition failedPartition = new TopicPartition(TOPIC, 0);
                pump.pause();
                harness.stopBroker(1);
                PartitionMetadata elected = harness.elect(failedPartition);
                if (elected.leaderId() != 2 || elected.epoch() != 1)
                    throw new IllegalStateException(
                            "clean election did not move orders-0 to broker 2 at epoch 1");
                System.out.println(
                        "ELECTED tp="
                                + failedPartition
                                + " leader="
                                + elected.leaderId()
                                + " epoch="
                                + elected.epoch()
                                + " metadata="
                                + elected);
                printSnapshot(harness, failedPartition, "AFTER_ELECTION");

                long followerLeo = harness.reconcile(3, failedPartition);
                if (!harness.admit(3, failedPartition))
                    throw new IllegalStateException("caught-up broker 3 was not admitted to ISR");
                System.out.println(
                        "RECOVERED broker=3 tp="
                                + failedPartition
                                + " leo="
                                + followerLeo
                                + " proof="
                                + harness.authority().snapshot(failedPartition).perReplicaLEO());
                printSnapshot(harness, failedPartition, "AFTER_ADMISSION_3");

                router.refresh(TOPIC);
                System.out.println(
                        "ROUTER refreshed leader="
                                + router.metadata(TOPIC).stream()
                                        .filter(metadata -> metadata.tp().equals(failedPartition))
                                        .findFirst()
                                        .orElseThrow()
                                        .leaderId());
                pump.resume();
                producer.send(
                        TOPIC,
                        keyForPartition(0, 2).getBytes(StandardCharsets.UTF_8),
                        "order-6".getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis());
                producer.send(
                        TOPIC,
                        keyForPartition(0, 2).getBytes(StandardCharsets.UTF_8),
                        "order-7".getBytes(StandardCharsets.UTF_8),
                        System.currentTimeMillis());
                System.out.println("PRODUCED post-election=" + receiptSummary(producer.flush()));

                pump.pause();
                harness.restartBroker(1);
                long restoredLeo = harness.reconcile(1, failedPartition);
                if (!harness.admit(1, failedPartition))
                    throw new IllegalStateException("recovered broker 1 was not admitted to ISR");
                System.out.println(
                        "RECOVERED broker=1 tp=" + failedPartition + " leo=" + restoredLeo);
                printSnapshot(harness, failedPartition, "AFTER_ADMISSION_1");
                pump.resume();

                consumeUntil(c1, c2, consumed, 8);
                Set<String> expected =
                        Set.of(
                                "order-0", "order-1", "order-2", "order-3", "order-4", "order-5",
                                "order-6", "order-7");
                if (!consumed.equals(expected))
                    throw new IllegalStateException(
                            "group output differs from produced values: " + consumed);
                printOffsets(harness.endpoint(2), "final", Map.of(0, 5L, 1, 3L));
                assertReplicaContents(harness, TOPIC, 2);
                assertReplicaDiskImages(harness, TOPIC);
                printMetadata(harness, "FINAL");
            }
        }
        System.out.println("SCENARIO PASS");
    }

    private static void consumeUntil(
            GroupConsumer c1, GroupConsumer c2, Set<String> seen, int expectedCount) {
        long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(10);
        while (seen.size() < expectedCount && System.nanoTime() < deadline) {
            consumeRound(c1, seen);
            consumeRound(c2, seen);
        }
        if (seen.size() != expectedCount)
            throw new CourseException(
                    ErrorCode.REQUEST_TIMEOUT,
                    "group consumers observed "
                            + seen.size()
                            + " of "
                            + expectedCount
                            + " records");
    }

    private static void consumeRound(GroupConsumer consumer, Set<String> seen) {
        Map<TopicPartition, List<LogRecord>> records = consumer.poll(32, 2_000_000);
        for (Map.Entry<TopicPartition, List<LogRecord>> entry : records.entrySet()) {
            for (LogRecord record : entry.getValue()) {
                String value = new String(record.data().value(), StandardCharsets.UTF_8);
                if (!seen.add(value))
                    throw new IllegalStateException("group delivered a duplicate value: " + value);
                System.out.println(
                        "CONSUMED tp="
                                + entry.getKey()
                                + " offset="
                                + record.offset()
                                + " value="
                                + value);
            }
        }
        consumer.commitSync();
    }

    private static void printAssignment(Endpoint controlEndpoint, String... members) {
        try (RpcClient client = new RpcClient(controlEndpoint, RPC_TIMEOUT_MILLIS)) {
            for (String member : members) {
                Messages.Reply reply =
                        client.call(new Messages.GroupAssignmentRequest(GROUP, member));
                requireSuccess(reply);
                if (!(reply.body() instanceof Messages.GroupBody body))
                    throw new IllegalStateException("group assignment returned unexpected body");
                GroupAssignment assignment = body.assignment();
                System.out.println(
                        "ASSIGNMENT generation="
                                + assignment.generation()
                                + " member="
                                + member
                                + " partitions="
                                + assignment.assignments().get(member));
            }
        }
    }

    private static void assertOldGenerationCommitRejected(Endpoint endpoint, TopicPartition tp) {
        GroupToken staleToken = new GroupToken(GROUP, "c1", 1);
        try (RpcClient client = new RpcClient(endpoint, RPC_TIMEOUT_MILLIS)) {
            Messages.Reply staleCommit =
                    client.call(
                            new Messages.CommitOffsetRequest(
                                    new OffsetKey(GROUP, tp), 99, staleToken));
            if (staleCommit.error() != ErrorCode.ILLEGAL_GENERATION)
                throw new IllegalStateException(
                        "stale group generation was not fenced: " + staleCommit.error());
            Messages.Reply fetched =
                    client.call(new Messages.FetchOffsetRequest(new OffsetKey(GROUP, tp)));
            requireSuccess(fetched);
            if (!(fetched.body() instanceof Messages.OffsetBody body)
                    || body.nextOffset().isPresent())
                throw new IllegalStateException("stale commit modified the stored offset");
            System.out.println(
                    "FENCED_COMMIT tp="
                            + tp
                            + " generation=1 error=ILLEGAL_GENERATION unchanged=true");
        }
    }

    private static void printOffsets(
            Endpoint controlEndpoint, String phase, Map<Integer, Long> expected) {
        try (RpcClient client = new RpcClient(controlEndpoint, RPC_TIMEOUT_MILLIS)) {
            for (int partition = 0; partition < 2; partition++) {
                TopicPartition tp = new TopicPartition(TOPIC, partition);
                Messages.Reply reply =
                        client.call(new Messages.FetchOffsetRequest(new OffsetKey(GROUP, tp)));
                requireSuccess(reply);
                if (!(reply.body() instanceof Messages.OffsetBody body))
                    throw new IllegalStateException("offset query returned unexpected body");
                long next = body.nextOffset().orElse(-1);
                System.out.println(
                        "OFFSET phase="
                                + phase
                                + " tp="
                                + tp
                                + " next="
                                + (next < 0 ? "<empty>" : next));
                if (next != expected.get(partition))
                    throw new IllegalStateException(
                            "committed offset mismatch for " + tp + ": " + next);
            }
        }
    }

    private static void printMetadata(ClusterHarness harness, String phase) {
        for (PartitionMetadata metadata : harness.authority().metadata(TOPIC))
            System.out.println(
                    "METADATA phase="
                            + phase
                            + " tp="
                            + metadata.tp()
                            + " leader="
                            + metadata.leaderId()
                            + " epoch="
                            + metadata.epoch()
                            + " replicas="
                            + metadata.replicas()
                            + " isr="
                            + metadata.isr());
    }

    private static void printSnapshot(ClusterHarness harness, TopicPartition tp, String phase) {
        ReplicationSnapshot snapshot = harness.snapshot(tp);
        System.out.println(
                "REPLICATION phase="
                        + phase
                        + " tp="
                        + tp
                        + " leader="
                        + snapshot.leaderId()
                        + " epoch="
                        + snapshot.epoch()
                        + " isr="
                        + snapshot.isr()
                        + " hw="
                        + snapshot.highWatermark()
                        + " leo="
                        + snapshot.perReplicaLEO());
    }

    private static void assertReplicaContents(
            ClusterHarness harness, String topic, int partitions) {
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition tp = new TopicPartition(topic, partition);
            List<LogRecord> expected = null;
            for (int brokerId : ClusterHarness.BROKER_IDS) {
                PartitionLogView view = new PartitionLogView(harness.partitionLog(brokerId, tp));
                List<LogRecord> records = view.records();
                if (expected == null) expected = records;
                else if (!sameRecords(expected, records))
                    throw new IllegalStateException(
                            "replica records differ at broker " + brokerId + " for " + tp);
            }
            System.out.println(
                    "REPLICA_CONTENTS tp="
                            + tp
                            + " records="
                            + (expected == null ? 0 : expected.size()));
        }
    }

    private static void assertReplicaDiskImages(ClusterHarness harness, String topic) {
        Map<Path, byte[]> baseline = diskImage(harness.catalog(1).root().resolve(topic));
        for (int brokerId : List.of(2, 3)) {
            Map<Path, byte[]> candidate =
                    diskImage(harness.catalog(brokerId).root().resolve(topic));
            if (!sameImage(baseline, candidate))
                throw new IllegalStateException(
                        "replica log/index disk bytes differ at broker " + brokerId);
        }
        System.out.println("DISK_IMAGES brokers=[1,2,3] equal=true");
    }

    private static Map<Path, byte[]> diskImage(Path root) {
        try (var paths = Files.walk(root)) {
            Map<Path, byte[]> image = new TreeMap<>();
            for (Path path : paths.filter(Files::isRegularFile).sorted().toList()) {
                if (path.toString().endsWith(".log") || path.toString().endsWith(".index"))
                    image.put(root.relativize(path), Files.readAllBytes(path));
            }
            return image;
        } catch (IOException exception) {
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR, "cannot compare replica disk images", exception);
        }
    }

    private static boolean sameImage(Map<Path, byte[]> left, Map<Path, byte[]> right) {
        if (!left.keySet().equals(right.keySet())) return false;
        for (Path path : left.keySet())
            if (!Arrays.equals(left.get(path), right.get(path))) return false;
        return true;
    }

    private static boolean sameRecords(List<LogRecord> left, List<LogRecord> right) {
        if (left.size() != right.size()) return false;
        for (int i = 0; i < left.size(); i++) {
            LogRecord a = left.get(i);
            LogRecord b = right.get(i);
            if (a.offset() != b.offset()
                    || a.data().timestamp() != b.data().timestamp()
                    || !Arrays.equals(a.data().key(), b.data().key())
                    || !Arrays.equals(a.data().value(), b.data().value())) return false;
        }
        return true;
    }

    private static String keyForPartition(int partition, int partitions) {
        Partitioner partitioner = new Partitioner();
        for (int candidate = 0; candidate < 10_000; candidate++) {
            String key = "order-key-" + candidate;
            if (partitioner.choose(key.getBytes(StandardCharsets.UTF_8), partitions) == partition)
                return key;
        }
        throw new IllegalStateException("cannot find a key for partition " + partition);
    }

    private static String receiptSummary(List<?> receipts) {
        return receipts.toString();
    }

    private static void requireSuccess(Messages.Reply reply) {
        if (reply.error() == ErrorCode.NONE) return;
        String message =
                reply.body() instanceof Messages.ErrorBody error
                        ? error.message()
                        : reply.error().name();
        throw new CourseException(reply.error(), message);
    }

    private static String safeMessage(Throwable failure) {
        return failure.getMessage() == null
                ? failure.getClass().getSimpleName()
                : failure.getMessage();
    }

    private static void deleteTree(Path root) {
        try (var paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList())
                Files.deleteIfExists(path);
        } catch (IOException failure) {
            System.err.println(
                    "could not remove temporary demo directory "
                            + root
                            + ": "
                            + failure.getMessage());
        }
    }

    private record PartitionLogView(io.simplekafka.storage.PartitionLog log) {
        private List<LogRecord> records() {
            long leo = log.logEndOffset();
            return leo == 0
                    ? List.of()
                    : log.read(log.logStartOffset(), Integer.MAX_VALUE, Integer.MAX_VALUE);
        }
    }

    private static final class ReplicationPump implements AutoCloseable {
        private final ClusterHarness harness;
        private final ScheduledExecutorService executor =
                Executors.newSingleThreadScheduledExecutor(
                        task -> {
                            Thread thread = new Thread(task, "simple-kafka-replication-pump");
                            thread.setDaemon(true);
                            return thread;
                        });
        private final AtomicBoolean enabled = new AtomicBoolean();
        private final AtomicReference<RuntimeException> failure = new AtomicReference<>();
        private final ReentrantLock tickLock = new ReentrantLock();

        private ReplicationPump(ClusterHarness harness) {
            this.harness = harness;
        }

        private void start() {
            enabled.set(true);
            executor.scheduleWithFixedDelay(this::tick, 0, 5, TimeUnit.MILLISECONDS);
        }

        private void tick() {
            tickLock.lock();
            try {
                if (!enabled.get() || failure.get() != null) return;
                for (PartitionMetadata metadata : harness.authority().metadata(TOPIC)) {
                    for (int brokerId : ClusterHarness.BROKER_IDS) {
                        if (brokerId == metadata.leaderId()
                                || !harness.authority().isOnline(brokerId)) continue;
                        try {
                            harness.replicateOnce(brokerId, metadata.tp(), 16, 2_000_000);
                            var tracker = harness.tracker(metadata.tp());
                            tracker.report(
                                    brokerId,
                                    metadata.epoch(),
                                    harness.partitionLog(brokerId, metadata.tp()).logEndOffset());
                            tracker.expireLagging();
                        } catch (RuntimeException exception) {
                            failure.compareAndSet(null, exception);
                            return;
                        }
                    }
                }
            } finally {
                tickLock.unlock();
            }
        }

        private void pause() {
            enabled.set(false);
            tickLock.lock();
            tickLock.unlock();
            checkFailure();
        }

        private void resume() {
            checkFailure();
            enabled.set(true);
        }

        private void checkFailure() {
            RuntimeException problem = failure.get();
            if (problem != null)
                throw new IllegalStateException("replication pump failed", problem);
        }

        /**
         * Stops the replication worker and checks whether it failed.
         *
         * @throws IllegalStateException if the worker does not stop within two seconds, the
         *     worker failed, or this thread is interrupted while stopping; interruption is
         *     restored before the exception is thrown
         */
        @Override
        public void close() {
            enabled.set(false);
            executor.shutdownNow();
            try {
                if (!executor.awaitTermination(2, TimeUnit.SECONDS))
                    throw new IllegalStateException("replication pump did not stop");
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
                throw new IllegalStateException(
                        "interrupted while stopping replication pump", exception);
            }
            checkFailure();
        }
    }
}
