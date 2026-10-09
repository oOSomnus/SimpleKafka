package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.client.DurableEffectStore;
import io.simplekafka.client.ProcessingLoop;
import io.simplekafka.client.RecordProcessor;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.lab.FaultProxy;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.BusinessEvent;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.transport.RpcClient;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;
import java.util.zip.CRC32C;

class Step31Test {
    private static final int RPC_TIMEOUT_MILLIS = 5_000;
    private static final int POLL_RECORDS = 10;
    private static final int POLL_BYTES = 65_536;
    private static final String ACCOUNT = "account-A";

    @TempDir Path root;

    @Test
    @DisplayName("Business events apply once and keep their deduplication identity after reopen")
    void appliesAndRecoversDurableBusinessEffects() throws Exception {
        Path directory = root.resolve("durable-effects");
        Path file = ledgerFile(directory);
        BusinessEvent first = new BusinessEvent("event-A", ACCOUNT, 10);
        BusinessEvent second = new BusinessEvent("event-B", ACCOUNT, -3);
        byte[] firstPayload = MessageCodec.encodeBusinessEvent(first);
        assertEquals(first, MessageCodec.decodeBusinessEvent(firstPayload));

        byte[] beforeReopen;
        try (DurableEffectStore store = new DurableEffectStore(directory)) {
            assertTrue(store.apply(first));
            assertFalse(store.apply(first));
            assertTrue(store.apply(second));
            assertEquals(7, store.balance(ACCOUNT));
            assertEquals(0, store.balance("missing-account"));
            assertEquals(List.of(first, second), ledgerEvents(file));
            store.recover();
            assertEquals(7, store.balance(ACCOUNT));
            assertFalse(store.apply(first));
            beforeReopen = Files.readAllBytes(file);
            store.close();
            assertCode(ErrorCode.STORAGE_ERROR, () -> store.balance(ACCOUNT));
            assertCode(ErrorCode.STORAGE_ERROR, () -> store.apply(second));
            assertCode(ErrorCode.STORAGE_ERROR, store::recover);
        }

        try (DurableEffectStore reopened = new DurableEffectStore(directory)) {
            assertEquals(7, reopened.balance(ACCOUNT));
            assertEquals(0, reopened.balance("missing-account"));
            assertFalse(reopened.apply(first));
            assertFalse(reopened.apply(second));
            assertArrayEquals(beforeReopen, Files.readAllBytes(file));
            assertEquals(List.of(first, second), ledgerEvents(file));
        }
    }

    @Test
    @DisplayName("Conflicting identities, invalid identifiers, and balance overflow have no effect")
    void rejectsConflictsInvalidIdentifiersAndOverflowWithoutAppending() throws Exception {
        Path directory = root.resolve("invalid-applications");
        Path file = ledgerFile(directory);
        BusinessEvent first = new BusinessEvent("stable-id", ACCOUNT, 10);
        BusinessEvent otherAccount = new BusinessEvent("stable-id", "account-B", 10);
        BusinessEvent otherDelta = new BusinessEvent("stable-id", ACCOUNT, 11);
        BusinessEvent maximum = new BusinessEvent("maximum", "limit-account", Long.MAX_VALUE);

        try (DurableEffectStore store = new DurableEffectStore(directory)) {
            assertTrue(store.apply(first));
            byte[] afterFirst = Files.readAllBytes(file);
            assertCode(ErrorCode.INVALID_REQUEST, () -> store.apply(otherAccount));
            assertCode(ErrorCode.INVALID_REQUEST, () -> store.apply(otherDelta));
            assertArrayEquals(afterFirst, Files.readAllBytes(file));
            assertEquals(10, store.balance(ACCOUNT));
            assertEquals(0, store.balance("account-B"));

            assertTrue(store.apply(maximum));
            byte[] afterMaximum = Files.readAllBytes(file);
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> store.apply(new BusinessEvent("overflow", "limit-account", 1)));
            assertArrayEquals(afterMaximum, Files.readAllBytes(file));
            assertEquals(Long.MAX_VALUE, store.balance("limit-account"));

            assertCode(ErrorCode.INVALID_REQUEST, () -> store.balance(null));
            assertCode(ErrorCode.INVALID_REQUEST, () -> store.balance(""));
            assertCode(ErrorCode.INVALID_REQUEST, () -> store.balance("é".repeat(128)));
            assertCode(ErrorCode.INVALID_REQUEST, () -> store.balance("\uD800"));
        }

        assertThrows(IllegalArgumentException.class, () -> new BusinessEvent("", ACCOUNT, 0));
        assertThrows(IllegalArgumentException.class, () -> new BusinessEvent("id", "", 0));
        assertThrows(
                IllegalArgumentException.class,
                () -> new BusinessEvent("é".repeat(128), ACCOUNT, 0));
        assertThrows(IllegalArgumentException.class, () -> new BusinessEvent("id", "\uD800", 0));
    }

    @Test
    @DisplayName("Business-event codec rejects invalid versions, identifiers, and trailing data")
    void businessEventCodecRejectsMalformedPayloads() {
        BusinessEvent expected = new BusinessEvent("event-17", "account-4", -12);
        byte[] encoded = MessageCodec.encodeBusinessEvent(expected);
        assertArrayEquals(businessPayload(utf8("event-17"), utf8("account-4"), -12), encoded);
        assertEquals(expected, MessageCodec.decodeBusinessEvent(encoded));
        BusinessEvent maximumIdentifiers =
                new BusinessEvent("é".repeat(127) + "a", "b".repeat(255), 0);
        assertEquals(
                maximumIdentifiers,
                MessageCodec.decodeBusinessEvent(
                        MessageCodec.encodeBusinessEvent(maximumIdentifiers)));

        byte[] trailing = Arrays.copyOf(encoded, encoded.length + 1);
        trailing[trailing.length - 1] = 99;
        List<byte[]> invalidPayloads =
                List.of(
                        new byte[] {2},
                        trailing,
                        new byte[] {1, 0, 0},
                        businessPayload(new byte[0], utf8("account"), 0),
                        businessPayload(utf8("event"), new byte[] {(byte) 0xc3, 0x28}, 0),
                        businessPayload(new byte[256], utf8("account"), 0));
        for (byte[] payload : invalidPayloads)
            assertCode(ErrorCode.INVALID_REQUEST, () -> MessageCodec.decodeBusinessEvent(payload));

        assertCode(ErrorCode.INVALID_REQUEST, () -> MessageCodec.encodeBusinessEvent(null));
    }

    @Test
    @DisplayName("Concurrent application serializes one durable business effect")
    void concurrentDuplicateApplicationsAppendOnceAndRecoverOnce() throws Exception {
        Path directory = root.resolve("concurrent-effects");
        Path file = ledgerFile(directory);
        BusinessEvent event = new BusinessEvent("concurrent-event", "parallel-account", 17);
        CountDownLatch ready = new CountDownLatch(2);
        CountDownLatch release = new CountDownLatch(1);
        ExecutorService workers = Executors.newFixedThreadPool(2);
        byte[] afterConcurrentApply;

        try (DurableEffectStore store = new DurableEffectStore(directory)) {
            try {
                Future<Boolean> first =
                        workers.submit(() -> applyAfterLatch(store, event, ready, release));
                Future<Boolean> second =
                        workers.submit(() -> applyAfterLatch(store, event, ready, release));
                assertTrue(
                        ready.await(5, TimeUnit.SECONDS), "both callers must reach the start gate");
                release.countDown();
                List<Boolean> outcomes =
                        List.of(first.get(5, TimeUnit.SECONDS), second.get(5, TimeUnit.SECONDS));
                assertEquals(1, outcomes.stream().filter(Boolean::booleanValue).count());
                assertEquals(1, outcomes.stream().filter(outcome -> !outcome).count());
                assertEquals(17, store.balance("parallel-account"));
                assertEquals(List.of(event), ledgerEvents(file));
                afterConcurrentApply = Files.readAllBytes(file);
            } finally {
                release.countDown();
                workers.shutdownNow();
                assertTrue(workers.awaitTermination(5, TimeUnit.SECONDS));
            }
        }

        try (DurableEffectStore reopened = new DurableEffectStore(directory)) {
            assertFalse(reopened.apply(event));
            assertEquals(17, reopened.balance("parallel-account"));
            assertArrayEquals(afterConcurrentApply, Files.readAllBytes(file));
            assertEquals(List.of(event), ledgerEvents(file));
        }
    }

    @Test
    @DisplayName(
            "Recovery counts identical historical events once and truncates an incomplete tail")
    void recoversRepeatedHistoryAndTruncatesIncompleteFinalRecord() throws Exception {
        Path repeatedDirectory = root.resolve("repeated-history");
        Path repeatedFile = ledgerFile(repeatedDirectory);
        BusinessEvent first = new BusinessEvent("historical-A", ACCOUNT, 10);
        BusinessEvent second = new BusinessEvent("historical-B", ACCOUNT, -3);
        byte[] repeatedPrefix =
                RecordBytes.concat(ordinaryRecord(0, first), ordinaryRecord(1, first));
        Files.createDirectories(repeatedDirectory);
        Files.write(repeatedFile, repeatedPrefix);

        try (DurableEffectStore recovered = new DurableEffectStore(repeatedDirectory)) {
            assertEquals(10, recovered.balance(ACCOUNT));
            assertFalse(recovered.apply(first));
            assertArrayEquals(repeatedPrefix, Files.readAllBytes(repeatedFile));
            assertTrue(recovered.apply(second));
            assertEquals(7, recovered.balance(ACCOUNT));
            assertEquals(List.of(first, first, second), ledgerEvents(repeatedFile));
        }

        Path partialDirectory = root.resolve("incomplete-tail");
        Path partialFile = ledgerFile(partialDirectory);
        byte[] firstRecord = ordinaryRecord(0, first);
        byte[] secondRecord = ordinaryRecord(1, second);
        byte[] incompleteSecond = Arrays.copyOf(secondRecord, secondRecord.length - 1);
        Files.createDirectories(partialDirectory);
        Files.write(partialFile, RecordBytes.concat(firstRecord, incompleteSecond));

        try (DurableEffectStore recovered = new DurableEffectStore(partialDirectory)) {
            assertEquals(10, recovered.balance(ACCOUNT));
            assertArrayEquals(firstRecord, Files.readAllBytes(partialFile));
            assertTrue(recovered.apply(second));
            assertEquals(7, recovered.balance(ACCOUNT));
            assertArrayEquals(
                    RecordBytes.concat(firstRecord, ordinaryRecord(1, second)),
                    Files.readAllBytes(partialFile));
            assertEquals(List.of(first, second), ledgerEvents(partialFile));
        }
    }

    @Test
    @DisplayName("Recovery rejects complete corrupt histories without rewriting their log bytes")
    void rejectsCorruptCompleteHistoryWithoutChangingItsLog() throws Exception {
        BusinessEvent first = new BusinessEvent("corrupt-A", ACCOUNT, 10);
        List<CorruptHistory> histories =
                List.of(
                        new CorruptHistory(
                                "conflicting-event-id",
                                RecordBytes.concat(
                                        ordinaryRecord(0, first),
                                        ordinaryRecord(
                                                1,
                                                new BusinessEvent(
                                                        "corrupt-A", "different-account", 10)))),
                        new CorruptHistory(
                                "wrong-key",
                                RecordBytes.record(
                                        0,
                                        utf8("unexpected-key"),
                                        MessageCodec.encodeBusinessEvent(first),
                                        0)),
                        new CorruptHistory(
                                "wrong-timestamp",
                                RecordBytes.record(
                                        0, null, MessageCodec.encodeBusinessEvent(first), 1)),
                        new CorruptHistory(
                                "unexpected-producer-stamp",
                                stampedRecord(0, null, MessageCodec.encodeBusinessEvent(first), 0)),
                        new CorruptHistory(
                                "malformed-complete-event-payload",
                                RecordBytes.record(0, null, malformedBusinessPayload(), 0)),
                        new CorruptHistory(
                                "historical-balance-overflow",
                                RecordBytes.concat(
                                        ordinaryRecord(
                                                0,
                                                new BusinessEvent(
                                                        "maximum",
                                                        "overflow-account",
                                                        Long.MAX_VALUE)),
                                        ordinaryRecord(
                                                1,
                                                new BusinessEvent(
                                                        "overflow", "overflow-account", 1)))));

        for (CorruptHistory history : histories) {
            Path directory = root.resolve("corrupt-" + history.name());
            Path file = ledgerFile(directory);
            Files.createDirectories(directory);
            Files.write(file, history.bytes());
            byte[] before = Files.readAllBytes(file);

            CourseException failure =
                    assertThrows(
                            CourseException.class,
                            () -> {
                                try (DurableEffectStore ignored =
                                        new DurableEffectStore(directory)) {
                                    throw new AssertionError("corrupt ledger was accepted");
                                }
                            },
                            history.name());
            assertEquals(ErrorCode.CORRUPT_RECORD, failure.code(), history.name());
            assertArrayEquals(before, Files.readAllBytes(file), history.name());
        }
    }

    @Test
    @DisplayName(
            "A rejected TCP commit replays callbacks while the durable ledger applies events once")
    void consumerCommitFailureReplaysCallbacksButNotDurableBusinessEffects() throws Exception {
        TopicPartition tp = new TopicPartition("step31-effects", 0);
        String group = "step31-effects-group";
        BusinessEvent first = new BusinessEvent("business-A", ACCOUNT, 10);
        BusinessEvent duplicateAtAnotherOffset = new BusinessEvent("business-A", ACCOUNT, 10);
        BusinessEvent second = new BusinessEvent("business-B", ACCOUNT, -3);
        List<BusinessEvent> inputEvents = List.of(first, duplicateAtAnotherOffset, second);
        List<RecordData> input = inputEvents.stream().map(Step31Test::recordData).toList();
        List<LogRecord> expectedRecords =
                List.of(
                        new LogRecord(0, input.get(0)),
                        new LogRecord(1, input.get(1)),
                        new LogRecord(2, input.get(2)));
        Path clusterDirectory = root.resolve("consumer-cluster");
        Path ledgerDirectory = root.resolve("consumer-ledger");
        Path ledger = ledgerFile(ledgerDirectory);
        ArrayList<Long> callbackOffsets = new ArrayList<>();

        try (ClusterHarness cluster = cluster(clusterDirectory, tp);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            assertEquals(new AppendResult(0, 3), produce(cluster, tp, input));
            for (int brokerId : List.of(2, 3)) {
                assertEquals(3, cluster.replicateOnce(brokerId, tp, POLL_RECORDS, POLL_BYTES));
                assertEquals(3, cluster.partitionLog(brokerId, tp).logEndOffset());
                cluster.tracker(tp).report(brokerId, 0, 3);
            }
            assertEquals(3, cluster.snapshot(tp).highWatermark());
            for (int brokerId : ClusterHarness.BROKER_IDS)
                assertEquals(
                        expectedRecords,
                        cluster.partitionLog(brokerId, tp).read(0, POLL_RECORDS, POLL_BYTES));

            proxy.start();
            proxy.rejectNextRequest(Api.COMMIT_OFFSET);
            byte[] afterFirstProcessing;
            try (DurableEffectStore store = new DurableEffectStore(ledgerDirectory);
                    SimpleConsumer consumer =
                            new SimpleConsumer(
                                    new RpcClient(proxy.endpoint(), RPC_TIMEOUT_MILLIS), group)) {
                consumer.resume(List.of(tp));
                assertEquals(0, consumer.position(tp));
                ProcessingLoop loop =
                        new ProcessingLoop(consumer, processor(tp, store, callbackOffsets));
                CourseException rejectedCommit =
                        assertThrows(
                                CourseException.class,
                                () -> loop.runOnce(POLL_RECORDS, POLL_BYTES));
                assertEquals(ErrorCode.REQUEST_TIMEOUT, rejectedCommit.code());
                assertEquals("injected pre-forward failure", rejectedCommit.getMessage());
                assertEquals(3, consumer.position(tp));
                assertEquals(List.of(0L, 1L, 2L), callbackOffsets);
                assertEquals(7, store.balance(ACCOUNT));
                assertEquals(List.of(first, second), ledgerEvents(ledger));
                assertEquals(OptionalLong.empty(), committedOffset(cluster, group, tp));
                afterFirstProcessing = Files.readAllBytes(ledger);
            }

            assertEquals(1, proxy.rejected(Api.COMMIT_OFFSET));
            assertEquals(0, proxy.forwarded(Api.COMMIT_OFFSET));
            assertFalse(proxy.armed());

            try (DurableEffectStore reopened = new DurableEffectStore(ledgerDirectory);
                    SimpleConsumer consumer =
                            new SimpleConsumer(cluster.client(1, RPC_TIMEOUT_MILLIS), group)) {
                consumer.resume(List.of(tp));
                assertEquals(0, consumer.position(tp));
                ProcessingLoop retry =
                        new ProcessingLoop(consumer, processor(tp, reopened, callbackOffsets));
                assertEquals(3, retry.runOnce(POLL_RECORDS, POLL_BYTES));
                assertEquals(3, consumer.position(tp));
                assertEquals(7, reopened.balance(ACCOUNT));
                assertArrayEquals(afterFirstProcessing, Files.readAllBytes(ledger));
                assertEquals(List.of(first, second), ledgerEvents(ledger));
            }

            assertEquals(List.of(0L, 1L, 2L, 0L, 1L, 2L), callbackOffsets);
            assertEquals(OptionalLong.of(3), committedOffset(cluster, group, tp));
            assertEquals(1, proxy.rejected(Api.COMMIT_OFFSET));
            assertEquals(0, proxy.forwarded(Api.COMMIT_OFFSET));
            assertEquals(
                    List.of(first, second),
                    ledgerEvents(ledger),
                    "the ledger contains each stable business identity once despite six callbacks");
        }
    }

    private static boolean applyAfterLatch(
            DurableEffectStore store,
            BusinessEvent event,
            CountDownLatch ready,
            CountDownLatch release)
            throws InterruptedException {
        ready.countDown();
        if (!release.await(5, TimeUnit.SECONDS))
            throw new AssertionError("concurrent apply start gate was not released");
        return store.apply(event);
    }

    private static RecordProcessor processor(
            TopicPartition expectedPartition,
            DurableEffectStore store,
            List<Long> callbackOffsets) {
        return (partition, record) -> {
            assertEquals(expectedPartition, partition);
            callbackOffsets.add(record.offset());
            store.apply(MessageCodec.decodeBusinessEvent(record.data().value()));
        };
    }

    private static ClusterHarness cluster(Path directory, TopicPartition tp) {
        ClusterHarness cluster = new ClusterHarness(directory, new AtomicLong(0)::get);
        cluster.start();
        cluster.createTopic(tp.topic(), 1, 2);
        return cluster;
    }

    private static AppendResult produce(
            ClusterHarness cluster, TopicPartition tp, List<RecordData> records) {
        try (RpcClient client = cluster.client(1, RPC_TIMEOUT_MILLIS)) {
            Messages.Reply reply =
                    client.call(
                            new Messages.ProduceRequest(
                                    tp, 0, Acks.LEADER, RPC_TIMEOUT_MILLIS, records));
            assertEquals(ErrorCode.NONE, reply.error());
            return ((Messages.ProduceBody) reply.body()).result();
        }
    }

    private static OptionalLong committedOffset(
            ClusterHarness cluster, String group, TopicPartition tp) {
        try (RpcClient client = cluster.client(1, RPC_TIMEOUT_MILLIS)) {
            Messages.Reply reply =
                    client.call(new Messages.FetchOffsetRequest(new OffsetKey(group, tp)));
            assertEquals(ErrorCode.NONE, reply.error());
            return ((Messages.OffsetBody) reply.body()).nextOffset();
        }
    }

    private static List<BusinessEvent> ledgerEvents(Path file) {
        return RecordBytes.readRecords(file).stream()
                .map(
                        record -> {
                            assertNull(record.data().key());
                            assertEquals(0, record.data().timestamp());
                            assertNull(record.data().producerStamp());
                            return MessageCodec.decodeBusinessEvent(record.data().value());
                        })
                .toList();
    }

    private static RecordData recordData(BusinessEvent event) {
        return new RecordData(null, MessageCodec.encodeBusinessEvent(event), 0);
    }

    private static byte[] ordinaryRecord(long offset, BusinessEvent event) {
        return RecordBytes.record(offset, null, MessageCodec.encodeBusinessEvent(event), 0);
    }

    private static byte[] stampedRecord(long offset, byte[] key, byte[] value, long timestamp) {
        int keyLength = key == null ? 0 : key.length;
        byte[] bytes = new byte[96 + keyLength + value.length];
        ByteBuffer output = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        output.putInt(bytes.length - Integer.BYTES);
        output.putInt(0);
        output.putLong(offset);
        output.putLong(timestamp);
        output.putInt(-2);
        output.putLong(7);
        output.putInt(0);
        output.putLong(0);
        output.putInt(1);
        output.putInt(0);
        output.put(new byte[32]);
        output.putInt(key == null ? -1 : key.length);
        output.putInt(value.length);
        if (key != null) output.put(key);
        output.put(value);
        CRC32C crc = new CRC32C();
        crc.update(bytes, Integer.BYTES * 2, bytes.length - Integer.BYTES * 2);
        ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(Integer.BYTES, (int) crc.getValue());
        return bytes;
    }

    private static byte[] businessPayload(byte[] eventId, byte[] account, long delta) {
        int payloadBytes =
                Integer.BYTES + eventId.length + Integer.BYTES + account.length + Long.BYTES;
        ByteBuffer output = ByteBuffer.allocate(1 + payloadBytes).order(ByteOrder.BIG_ENDIAN);
        output.put((byte) 1);
        output.putInt(eventId.length).put(eventId);
        output.putInt(account.length).put(account);
        output.putLong(delta);
        return output.array();
    }

    private static byte[] malformedBusinessPayload() {
        byte[] eventId = utf8("broken-event");
        byte[] account = utf8("broken-account");
        int payloadBytes =
                Integer.BYTES + eventId.length + Integer.BYTES + account.length + Integer.BYTES;
        ByteBuffer output = ByteBuffer.allocate(1 + payloadBytes).order(ByteOrder.BIG_ENDIAN);
        output.put((byte) 1);
        output.putInt(eventId.length).put(eventId);
        output.putInt(account.length).put(account);
        output.putInt(17); // Delta is incomplete while the containing disk record remains complete.
        return output.array();
    }

    private static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    private static Path ledgerFile(Path directory) {
        return directory.resolve("00000000000000000000.log");
    }

    private static void assertCode(ErrorCode expected, Executable action) {
        CourseException failure = assertThrows(CourseException.class, action);
        assertEquals(expected, failure.code());
    }

    private record CorruptHistory(String name, byte[] bytes) {}
}
