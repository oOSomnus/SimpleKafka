package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.client.IdempotentProducer;
import io.simplekafka.client.MetadataRouter;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.lab.FaultProxy;
import io.simplekafka.lab.LabSupport;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProduceReceipt;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import io.simplekafka.replication.IdempotentAppender;
import io.simplekafka.replication.ReplicatedPartition;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.storage.RecordCodec;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.function.Executable;
import org.junit.jupiter.api.io.TempDir;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicLong;

class Step30Test {
    @TempDir Path root;

    @Test
    @DisplayName("Idempotent producer preserves receipts and sequences across real TCP requests")
    void idempotentProducerUsesIndependentPerPartitionAndProducerSequences() throws Exception {
        TopicPartition firstPartition = new TopicPartition("step30-producer", 0);
        TopicPartition secondPartition = new TopicPartition("step30-producer", 1);
        List<RecordData> firstBatch = List.of(record("first-key", "first-value", 101));
        List<RecordData> nextBatch = List.of(record("next-key", "next-value", 102));
        List<RecordData> otherPartitionBatch = List.of(record("other-key", "other-value", 103));
        List<RecordData> otherProducerBatch = List.of(record("pid-key", "pid-value", 104));

        try (ClusterHarness cluster = cluster(root.resolve("producer"), firstPartition.topic(), 2);
                MetadataRouter router = router(cluster);
                IdempotentProducer producer = new IdempotentProducer(router, 7, 0);
                IdempotentProducer otherProducer = new IdempotentProducer(router, 8, 0)) {
            assertEquals(
                    new ProduceReceipt(firstPartition, 0, 1),
                    producer.send(firstPartition, firstBatch, Acks.LEADER, 5_000));
            Map<Integer, Long> leoAfterFirst = leos(cluster, firstPartition);
            Map<String, String> bytesAfterFirst = snapshotFiles(root.resolve("producer"));

            Messages.Reply repeated =
                    idempotentRequest(
                            cluster, 1, firstPartition, 0, Acks.LEADER, 5_000, 7, 0, 0, firstBatch);
            assertEquals(ErrorCode.NONE, repeated.error());
            assertEquals(new AppendResult(0, 1), produceResult(repeated));
            assertEquals(leoAfterFirst, leos(cluster, firstPartition));
            assertEquals(bytesAfterFirst, snapshotFiles(root.resolve("producer")));

            assertEquals(
                    new ProduceReceipt(firstPartition, 1, 2),
                    producer.send(firstPartition, nextBatch, Acks.LEADER, 5_000));
            assertEquals(
                    new ProduceReceipt(secondPartition, 0, 1),
                    producer.send(secondPartition, otherPartitionBatch, Acks.LEADER, 5_000));
            assertEquals(
                    new ProduceReceipt(firstPartition, 2, 3),
                    otherProducer.send(firstPartition, otherProducerBatch, Acks.LEADER, 5_000));

            List<LogRecord> expectedFirstPartition = new ArrayList<>();
            expectedFirstPartition.addAll(expectedStamped(0, 7, 0, 0, firstBatch));
            expectedFirstPartition.addAll(expectedStamped(1, 7, 0, 1, nextBatch));
            expectedFirstPartition.addAll(expectedStamped(2, 8, 0, 0, otherProducerBatch));
            assertEquals(
                    List.copyOf(expectedFirstPartition),
                    diskRecords(root.resolve("producer"), 1, firstPartition));
            assertEquals(
                    expectedStamped(0, 7, 0, 0, otherPartitionBatch),
                    diskRecords(root.resolve("producer"), 2, secondPartition));
            assertEquals(Map.of(1, 3L, 2, 0L, 3, 0L), leos(cluster, firstPartition));
            assertEquals(Map.of(1, 0L, 2, 1L, 3, 0L), leos(cluster, secondPartition));
        }
    }

    @Test
    @DisplayName("An unresolved batch blocks only its partition until explicit retry")
    void unresolvedBatchIsPerPartitionAndRequiresExplicitRetry() throws Exception {
        TopicPartition pendingPartition = new TopicPartition("step30-pending", 0);
        TopicPartition independentPartition = new TopicPartition("step30-pending", 1);
        List<RecordData> pendingBatch = List.of(record("pending", "unknown", 351));
        List<RecordData> blockedBatch = List.of(record("pending", "must-not-send", 352));
        List<RecordData> independentBatch = List.of(record("independent", "allowed", 353));
        Path clusterRoot = root.resolve("pending");

        try (ClusterHarness cluster = cluster(clusterRoot, pendingPartition.topic(), 2);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1));
                MetadataRouter router =
                        new MetadataRouter(
                                ClusterHarness.BROKER_IDS.stream().map(cluster::endpoint).toList(),
                                endpoint ->
                                        new RpcClient(
                                                endpoint.equals(cluster.endpoint(1))
                                                        ? proxy.endpoint()
                                                        : endpoint,
                                                5_000));
                IdempotentProducer producer = new IdempotentProducer(router, 9, 0)) {
            proxy.start();
            proxy.dropNextReply(Api.IDEMPOTENT_PRODUCE);
            CourseException unknown =
                    assertThrows(
                            CourseException.class,
                            () ->
                                    producer.send(
                                            pendingPartition, pendingBatch, Acks.LEADER, 5_000));
            assertEquals(ErrorCode.REQUEST_TIMEOUT, unknown.code());
            assertEquals(1, proxy.forwarded(Api.IDEMPOTENT_PRODUCE));
            Map<String, String> afterUnknown = snapshotFiles(clusterRoot);

            CourseException blocked =
                    assertThrows(
                            CourseException.class,
                            () ->
                                    producer.send(
                                            pendingPartition, blockedBatch, Acks.LEADER, 5_000));
            assertEquals(ErrorCode.INVALID_REQUEST, blocked.code());
            assertEquals(afterUnknown, snapshotFiles(clusterRoot));
            assertEquals(1, proxy.forwarded(Api.IDEMPOTENT_PRODUCE));

            assertEquals(
                    new ProduceReceipt(independentPartition, 0, 1),
                    producer.send(independentPartition, independentBatch, Acks.LEADER, 5_000));
            assertEquals(
                    expectedStamped(0, 9, 0, 0, independentBatch),
                    diskRecords(clusterRoot, 2, independentPartition));

            assertEquals(
                    new ProduceReceipt(pendingPartition, 0, 1), producer.retry(pendingPartition));
            List<LogRecord> expectedPending = expectedStamped(0, 9, 0, 0, pendingBatch);
            assertEquals(expectedPending, diskRecords(clusterRoot, 1, pendingPartition));
            assertEquals(2, proxy.forwarded(Api.IDEMPOTENT_PRODUCE));
            assertEquals(1, proxy.dropped(Api.IDEMPOTENT_PRODUCE));
            assertEquals(0, proxy.rejected(Api.IDEMPOTENT_PRODUCE));
        }
    }

    @Test
    @DisplayName("API 11 request and stamped FETCH records round-trip through the protocol codec")
    void idempotentApiAndStampedFetchWireFormatsRoundTrip() {
        TopicPartition tp = new TopicPartition("step30-wire", 0);
        RecordData requestRecord = record("wire-key", "wire-value", 202);
        Messages.IdempotentProduceRequest request =
                new Messages.IdempotentProduceRequest(
                        tp, 4, Acks.ALL, 8_000, 90, 3, 12, List.of(requestRecord));
        assertEquals(Api.IDEMPOTENT_PRODUCE, request.apiId());
        assertTrue(Api.known((short) 11));
        assertEquals(
                request,
                MessageCodec.decodeRequest(
                        Api.IDEMPOTENT_PRODUCE, MessageCodec.encodeRequest(request)));

        Messages.ProduceBody receipt = new Messages.ProduceBody(new AppendResult(31, 32));
        assertEquals(
                receipt,
                MessageCodec.decodeReply(
                        Api.IDEMPOTENT_PRODUCE, MessageCodec.encodeReply(receipt)));

        byte[] hash = hashBytes();
        RecordData stamped =
                new RecordData(
                        new byte[0],
                        bytes("fetched"),
                        203,
                        new ProducerStamp(90, 3, 12, 1, 0, hash));
        LogRecord record = new LogRecord(31, stamped);
        Messages.FetchBody fetch = new Messages.FetchBody(List.of(record), 0, 32, 32, 4);
        assertEquals(fetch, MessageCodec.decodeReply(Api.FETCH, MessageCodec.encodeReply(fetch)));
        Messages.ReplicaFetchBody replicaFetch =
                new Messages.ReplicaFetchBody(List.of(record), 4, 32, 33);
        assertEquals(
                replicaFetch,
                MessageCodec.decodeReply(
                        Api.REPLICA_FETCH, MessageCodec.encodeReply(replicaFetch)));
    }

    @Test
    @DisplayName("Single-broker backends explicitly reject the idempotent produce API")
    void singleBrokerRejectsIdempotentProduceWithoutAppending() {
        TopicPartition tp = new TopicPartition("step30-single", 0);
        List<RecordData> batch = List.of(record("single", "unsupported", 251));
        try (BrokerHarness broker =
                        BrokerHarness.single(root.resolve("single-broker"), 1, 0, tp.topic(), 1);
                RpcClient client = new RpcClient(broker.endpoint(), 5_000)) {
            Messages.Reply reply =
                    client.call(
                            new Messages.IdempotentProduceRequest(
                                    tp, 0, Acks.LEADER, 5_000, 1, 0, 0, batch));
            assertReplyCode(ErrorCode.INVALID_REQUEST, reply);
            assertEquals(0, broker.catalog().partition(tp).logEndOffset());
        }
    }

    @Test
    @DisplayName("Invalid, empty, stamped, and oversized producer batches have no side effects")
    void producerRejectsInvalidBatchesBeforeChangingSequenceOrLogs() throws Exception {
        TopicPartition tp = new TopicPartition("step30-invalid", 0);
        List<RecordData> ordinary = List.of(record("valid", "after-invalid", 301));
        RecordData stamped =
                new RecordData(
                        null,
                        bytes("already-stamped"),
                        302,
                        new ProducerStamp(1, 0, 0, 1, 0, hashBytes()));
        RecordData tooLarge = new RecordData(null, new byte[1_048_485], 303);

        try (ClusterHarness cluster = cluster(root.resolve("invalid"), tp.topic(), 1);
                MetadataRouter router = router(cluster);
                IdempotentProducer producer = new IdempotentProducer(router, 11, 0)) {
            Map<Integer, Long> initialLeo = leos(cluster, tp);
            Map<String, String> initialFiles = snapshotFiles(root.resolve("invalid"));

            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> producer.send(tp, List.of(), Acks.LEADER, 5_000));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> producer.send(tp, List.of(stamped), Acks.LEADER, 5_000));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> producer.send(tp, List.of(tooLarge), Acks.LEADER, 5_000));

            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, -1, 0, 0, ordinary)));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, 11, 0, 0, List.of())));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, 11, 0, 0, List.of(stamped))));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () ->
                            MessageCodec.encodeRequest(
                                    new Messages.ProduceRequest(
                                            tp, 0, Acks.LEADER, 5_000, List.of(stamped))));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, 11, 0, 0, List.of(tooLarge))));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, 11, -1, 0, ordinary)));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, 11, 0, -1, ordinary)));
            assertCode(
                    ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.encodeRequest(request(tp, 11, 0, Long.MAX_VALUE, ordinary)));
            assertEquals(initialLeo, leos(cluster, tp));
            assertEquals(initialFiles, snapshotFiles(root.resolve("invalid")));

            assertEquals(
                    new ProduceReceipt(tp, 0, 1),
                    producer.send(tp, ordinary, Acks.LEADER, 5_000),
                    "rejected batches must not consume the producer's first sequence");
            assertEquals(
                    expectedStamped(0, 11, 0, 0, ordinary),
                    diskRecords(root.resolve("invalid"), 1, tp));
        }
    }

    @Test
    @DisplayName(
            "Sequence gaps, old epochs, and same-sequence conflicts leave durable history unchanged")
    void sequenceErrorsDoNotAppendOrInstallAnUnpersistedEpoch() throws Exception {
        TopicPartition tp = new TopicPartition("step30-sequences", 0);
        List<RecordData> first = List.of(record("identity", "first", 401));

        try (ClusterHarness cluster = cluster(root.resolve("sequences"), tp.topic(), 1)) {
            assertEquals(
                    new AppendResult(0, 1),
                    produceResult(
                            idempotentRequest(
                                    cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 3, 0, first)));
            Map<Integer, Long> leoAfterFirst = leos(cluster, tp);
            Map<String, String> filesAfterFirst = snapshotFiles(root.resolve("sequences"));

            assertReplyCode(
                    ErrorCode.FENCED_PRODUCER_EPOCH,
                    idempotentRequest(cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 2, 1, first));
            assertUnchanged(cluster, tp, root.resolve("sequences"), leoAfterFirst, filesAfterFirst);

            assertReplyCode(
                    ErrorCode.OUT_OF_ORDER_SEQUENCE,
                    idempotentRequest(cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 3, 2, first));
            assertUnchanged(cluster, tp, root.resolve("sequences"), leoAfterFirst, filesAfterFirst);

            assertReplyCode(
                    ErrorCode.OUT_OF_ORDER_SEQUENCE,
                    idempotentRequest(cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 4, 1, first));
            assertUnchanged(cluster, tp, root.resolve("sequences"), leoAfterFirst, filesAfterFirst);

            List<RecordData> conflicting = List.of(record("identity", "different", 401));
            assertReplyCode(
                    ErrorCode.DUPLICATE_SEQUENCE_CONFLICT,
                    idempotentRequest(
                            cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 3, 0, conflicting));
            assertUnchanged(cluster, tp, root.resolve("sequences"), leoAfterFirst, filesAfterFirst);

            List<RecordData> wrongBoundary =
                    List.of(first.getFirst(), record("identity-2", "different-boundary", 402));
            assertReplyCode(
                    ErrorCode.OUT_OF_ORDER_SEQUENCE,
                    idempotentRequest(
                            cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 3, 0, wrongBoundary));
            assertUnchanged(cluster, tp, root.resolve("sequences"), leoAfterFirst, filesAfterFirst);

            List<RecordData> second = List.of(record("identity", "second", 402));
            assertEquals(
                    new AppendResult(1, 2),
                    produceResult(
                            idempotentRequest(
                                    cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 3, 1, second)),
                    "a rejected higher epoch must not fence the current durable epoch");
            List<RecordData> newEpoch = List.of(record("identity", "new-epoch", 403));
            assertEquals(
                    new AppendResult(2, 3),
                    produceResult(
                            idempotentRequest(
                                    cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 4, 0, newEpoch)));
            Map<Integer, Long> leoAfterNewEpoch = leos(cluster, tp);
            Map<String, String> filesAfterNewEpoch = snapshotFiles(root.resolve("sequences"));
            assertReplyCode(
                    ErrorCode.FENCED_PRODUCER_EPOCH,
                    idempotentRequest(cluster, 1, tp, 0, Acks.LEADER, 5_000, 17, 3, 2, second));
            assertUnchanged(
                    cluster, tp, root.resolve("sequences"), leoAfterNewEpoch, filesAfterNewEpoch);

            List<LogRecord> expected = new ArrayList<>();
            expected.addAll(expectedStamped(0, 17, 3, 0, first));
            expected.addAll(expectedStamped(1, 17, 3, 1, second));
            expected.addAll(expectedStamped(2, 17, 4, 0, newEpoch));
            assertEquals(List.copyOf(expected), diskRecords(root.resolve("sequences"), 1, tp));
        }
    }

    @Test
    @DisplayName(
            "Independent disk oracle preserves stamped fields and validates CRC and boundaries")
    void stampedRecordDiskBytesAreIndependentAndStructurallyChecked() throws Exception {
        byte[] hash = hashBytes();
        RecordData nullKey =
                new RecordData(
                        null, bytes("null-key"), 501, new ProducerStamp(71, 2, 9, 2, 0, hash));
        RecordData emptyKey =
                new RecordData(
                        new byte[0],
                        bytes("empty-key"),
                        502,
                        new ProducerStamp(71, 2, 9, 2, 1, hash));
        byte[] nullKeyOracle = RecordBytes.record(4, nullKey);
        byte[] emptyKeyOracle = RecordBytes.record(5, emptyKey);
        assertArrayEquals(nullKeyOracle, encoded(new LogRecord(4, nullKey)));
        assertArrayEquals(emptyKeyOracle, encoded(new LogRecord(5, emptyKey)));
        assertEquals(-2, ByteBuffer.wrap(nullKeyOracle).order(ByteOrder.BIG_ENDIAN).getInt(24));
        assertEquals(71L, ByteBuffer.wrap(nullKeyOracle).order(ByteOrder.BIG_ENDIAN).getLong(28));
        assertArrayEquals(
                RecordBytes.record(6, null, bytes("ordinary"), 503),
                RecordBytes.record(6, new RecordData(null, bytes("ordinary"), 503)),
                "the new overload must retain the old ordinary encoding byte-for-byte");
        assertArrayEquals(
                RecordBytes.record(6, null, bytes("ordinary"), 503),
                encoded(new LogRecord(6, new RecordData(null, bytes("ordinary"), 503))));

        Path file = root.resolve("oracle.log");
        Files.write(file, RecordBytes.concat(nullKeyOracle, emptyKeyOracle));
        assertEquals(
                List.of(new LogRecord(4, nullKey), new LogRecord(5, emptyKey)),
                RecordBytes.readRecords(file));

        ByteBuffer positioned = ByteBuffer.allocate(nullKeyOracle.length + 7);
        positioned.position(3);
        positioned.put(nullKeyOracle);
        positioned.position(3);
        assertEquals(new LogRecord(4, nullKey), RecordCodec.decode(positioned));
        assertEquals(3 + nullKeyOracle.length, positioned.position());

        byte[] damagedStamp = RecordBytes.withField(nullKeyOracle, 28, 72, 8, false);
        ByteBuffer crcFailure = ByteBuffer.wrap(damagedStamp);
        assertCode(ErrorCode.CORRUPT_RECORD, () -> RecordCodec.decode(crcFailure));
        assertEquals(0, crcFailure.position(), "CRC failure must not advance the source buffer");

        byte[] malformedStamp = RecordBytes.withField(nullKeyOracle, 52, 2, 4, true);
        ByteBuffer malformed = ByteBuffer.wrap(malformedStamp);
        assertCode(ErrorCode.CORRUPT_RECORD, () -> RecordCodec.decode(malformed));
        assertEquals(
                0, malformed.position(), "a complete invalid stamp must not advance the buffer");

        byte[] truncatedBytes = java.util.Arrays.copyOf(nullKeyOracle, nullKeyOracle.length - 1);
        ByteBuffer truncated = ByteBuffer.wrap(truncatedBytes);
        assertCode(ErrorCode.INVALID_REQUEST, () -> RecordCodec.decode(truncated));
        assertEquals(
                0, truncated.position(), "a truncated declared body must not advance the buffer");

        ProducerStamp maximumStamp = new ProducerStamp(72, 0, 0, 1, 0, hashBytes());
        RecordData maximum = new RecordData(null, new byte[1_048_484], 504, maximumStamp);
        byte[] maximumOracle = RecordBytes.record(0, maximum);
        assertEquals(1_048_580, maximumOracle.length);
        assertEquals(
                1_048_576, ByteBuffer.wrap(maximumOracle).order(ByteOrder.BIG_ENDIAN).getInt());
        assertArrayEquals(maximumOracle, encoded(new LogRecord(0, maximum)));
        RecordData oneByteTooLarge = new RecordData(null, new byte[1_048_485], 505, maximumStamp);
        assertCode(
                ErrorCode.INVALID_REQUEST,
                () -> RecordCodec.encode(new LogRecord(0, oneByteTooLarge)));
    }

    @Test
    @DisplayName(
            "Incomplete batches recover, reject interleaving, and resume only the original suffix")
    void incompleteBatchSurvivesRecoveryAndCanOnlyBeCompletedByItsOriginalRequest()
            throws Exception {
        Path directory = root.resolve("partial");
        List<RecordData> records =
                List.of(
                        record("partial-0", "zero", 601),
                        record("partial-1", "one", 602),
                        record("partial-2", "two", 603));
        byte[] hash = independentFingerprint(records);
        try (PartitionLog initial = new PartitionLog(directory, 4_096, 2)) {
            assertEquals(
                    new AppendResult(0, 2),
                    initial.append(stampedSlice(77, 0, 0, records, hash, 0, 2)));
            assertEquals(1, initial.tailProducerStamp().batchIndex());
        }

        try (PartitionLog recovered = new PartitionLog(directory, 4_096, 2)) {
            assertEquals(2, recovered.logEndOffset());

            Map<String, String> prefixBytes = snapshotFiles(directory);
            assertCode(
                    ErrorCode.INCOMPLETE_BATCH,
                    () -> recovered.append(List.of(record("ordinary", "cannot-interleave", 604))));
            assertEquals(prefixBytes, snapshotFiles(directory));
            assertCode(
                    ErrorCode.INCOMPLETE_BATCH,
                    () -> recovered.append(stampedBatch(78, 0, 0, records)));
            assertEquals(prefixBytes, snapshotFiles(directory));
            assertCode(
                    ErrorCode.INCOMPLETE_BATCH,
                    () -> recovered.append(stampedBatch(77, 1, 0, records)));
            assertEquals(prefixBytes, snapshotFiles(directory));

            IdempotentAppender differentProducer = new IdempotentAppender(recovered);
            assertCode(
                    ErrorCode.INCOMPLETE_BATCH, () -> differentProducer.append(78, 0, 0, records));
            assertEquals(prefixBytes, snapshotFiles(directory));
            List<RecordData> conflicting =
                    List.of(records.get(0), record("partial-1", "changed", 602), records.get(2));
            assertCode(
                    ErrorCode.DUPLICATE_SEQUENCE_CONFLICT,
                    () -> new IdempotentAppender(recovered).append(77, 0, 0, conflicting));
            assertEquals(prefixBytes, snapshotFiles(directory));

            IdempotentAppender originalProducer = new IdempotentAppender(recovered);
            originalProducer.recover();
            AppendResult completed = originalProducer.append(77, 0, 0, records);
            assertEquals(new AppendResult(0, 3), completed);
            List<LogRecord> expected = expectedStamped(0, 77, 0, 0, records);
            assertEquals(expected, RecordBytes.readRecords(logFile(directory, 0)));
            Map<String, String> completedBytes = snapshotFiles(directory);
            assertEquals(completed, originalProducer.append(77, 0, 0, records));
            assertEquals(completedBytes, snapshotFiles(directory));

            recovered.truncateTo(1);
            assertEquals(1, recovered.logEndOffset());
            assertEquals(
                    new AppendResult(0, 3),
                    originalProducer.append(77, 0, 0, records),
                    "truncation inside a batch must rebuild state and continue at its stored suffix");
            assertEquals(expected, RecordBytes.readRecords(logFile(directory, 0)));
            assertEquals(3, recovered.logEndOffset());
        }
    }

    @Test
    @DisplayName("A truncate followed by same-LEO replacement invalidates a cached receipt")
    void sameLeoRewriteCannotReuseOldProducerReceipt() {
        Path directory = root.resolve("same-leo-rewrite");
        RecordData first = record("rewrite-0", "stable-prefix", 701);
        RecordData oldSecond = record("rewrite-1", "old-tail", 702);
        RecordData replacement = record("rewrite-1", "new-tail", 703);
        try (PartitionLog log = new PartitionLog(directory, 4_096, 2)) {
            IdempotentAppender appender = new IdempotentAppender(log);
            assertEquals(new AppendResult(0, 1), appender.append(81, 0, 0, List.of(first)));
            AppendResult oldReceipt = appender.append(81, 0, 1, List.of(oldSecond));
            assertEquals(new AppendResult(1, 2), oldReceipt);

            long prefixBefore = log.prefixVersion();
            log.truncateTo(1);
            assertEquals(prefixBefore + 1, log.prefixVersion());
            assertEquals(new AppendResult(1, 2), log.append(List.of(replacement)));
            assertEquals(2, log.logEndOffset(), "the replacement restores the prior LEO");

            AppendResult newReceipt = appender.append(81, 0, 1, List.of(replacement));
            assertEquals(new AppendResult(2, 3), newReceipt);
            assertFalse(oldReceipt.equals(newReceipt));
            assertEquals(
                    List.of(
                            expectedStamped(0, 81, 0, 0, List.of(first)).getFirst(),
                            new LogRecord(1, replacement),
                            expectedStamped(2, 81, 0, 1, List.of(replacement)).getFirst()),
                    RecordBytes.readRecords(logFile(directory, 0)));
        }
    }

    @Test
    @DisplayName("Producer-state recovery rejects a complete batch with a mismatched fingerprint")
    void recoveryRejectsCompleteBatchWithIncorrectFingerprint() throws Exception {
        Path directory = root.resolve("bad-fingerprint");
        RecordData expected = record("fingerprint-key", "expected-payload", 751);
        List<RecordData> originalBatch = List.of(expected);
        byte[] expectedHash = independentFingerprint(originalBatch);
        RecordData damaged =
                new RecordData(
                        expected.key(),
                        bytes("damaged-payload"),
                        expected.timestamp(),
                        new ProducerStamp(82, 0, 0, 1, 0, expectedHash));

        try (PartitionLog initial = new PartitionLog(directory, 4_096, 2)) {
            assertEquals(new AppendResult(0, 1), initial.append(List.of(damaged)));
        }
        Map<String, String> damagedBytes = snapshotFiles(directory);

        try (PartitionLog recovered = new PartitionLog(directory, 4_096, 2)) {
            IdempotentAppender appender = new IdempotentAppender(recovered);
            assertCode(ErrorCode.CORRUPT_RECORD, appender::recover);
            assertEquals(damagedBytes, snapshotFiles(directory));
            assertCode(ErrorCode.CORRUPT_RECORD, () -> appender.append(82, 0, 0, originalBatch));
            assertEquals(damagedBytes, snapshotFiles(directory));
        }
    }

    @Test
    @DisplayName("Idempotent history expires after retention, including after reopening the log")
    void retainedPrefixCannotServeCachedOrRecoveredProducerReceipts() {
        Path directory = root.resolve("retention");
        RecordData first = record("retained-0", "zero", 801);
        RecordData second = record("retained-1", "one", 802);
        try (PartitionLog log = new PartitionLog(directory, 1, 1)) {
            IdempotentAppender appender = new IdempotentAppender(log);
            assertEquals(new AppendResult(0, 1), appender.append(91, 0, 0, List.of(first)));
            assertEquals(new AppendResult(1, 2), appender.append(91, 0, 1, List.of(second)));
            assertEquals(1, log.deleteBefore(1));
            assertEquals(1, log.logStartOffset());
            assertCode(
                    ErrorCode.PRODUCER_STATE_EXPIRED,
                    () -> appender.append(91, 0, 1, List.of(second)));
        }
        try (PartitionLog reopened = new PartitionLog(directory, 1, 1)) {
            assertEquals(1, reopened.logStartOffset());
            assertCode(
                    ErrorCode.PRODUCER_STATE_EXPIRED,
                    () -> new IdempotentAppender(reopened).append(91, 0, 1, List.of(second)));
        }
    }

    @Test
    @DisplayName("ALL retries time out without duplicate append, then honor ISR and leader epochs")
    void allAckDuplicateWaitsForHwAndStillRequiresMinIsrAndCurrentEpoch() throws Exception {
        TopicPartition tp = new TopicPartition("step30-all", 0);
        List<RecordData> batch = List.of(record("all-key", "all-value", 901));
        AtomicLong clock = new AtomicLong(0);

        try (ClusterHarness cluster = cluster(root.resolve("all"), tp.topic(), 1, clock);
                MetadataRouter router = router(cluster);
                IdempotentProducer producer = new IdempotentProducer(router, 101, 0)) {
            CourseException firstTimeout =
                    assertThrows(
                            CourseException.class, () -> producer.send(tp, batch, Acks.ALL, 0));
            assertEquals(ErrorCode.REQUEST_TIMEOUT, firstTimeout.code());
            assertEquals(Map.of(1, 1L, 2, 0L, 3, 0L), leos(cluster, tp));
            assertEquals(0, cluster.snapshot(tp).highWatermark());
            Map<String, String> afterFirstTimeout = snapshotFiles(root.resolve("all"));

            CourseException duplicateTimeout =
                    assertThrows(CourseException.class, () -> producer.retry(tp));
            assertEquals(ErrorCode.REQUEST_TIMEOUT, duplicateTimeout.code());
            assertEquals(Map.of(1, 1L, 2, 0L, 3, 0L), leos(cluster, tp));
            assertEquals(afterFirstTimeout, snapshotFiles(root.resolve("all")));

            LabSupport.replicateAndReport(cluster, tp);
            assertEquals(1, cluster.snapshot(tp).highWatermark());
            assertEquals(Map.of(1, 1L, 2, 1L, 3, 1L), leos(cluster, tp));
            assertEquals(new ProduceReceipt(tp, 0, 1), producer.retry(tp));
            Map<String, String> afterAcknowledgedDuplicate = snapshotFiles(root.resolve("all"));
            assertEquals(Map.of(1, 1L, 2, 1L, 3, 1L), leos(cluster, tp));
            assertEquals(afterAcknowledgedDuplicate, snapshotFiles(root.resolve("all")));

            cluster.stopBroker(1);
            assertEquals(2, cluster.elect(tp).leaderId());
            assertEquals(1, cluster.snapshot(tp).epoch());
            assertEquals(List.of(2), cluster.snapshot(tp).isr());
            assertTrue(cluster.snapshot(tp).isr().size() < 2);
            Map<Integer, Long> afterElectionLeo = leos(cluster, tp);
            Map<String, String> afterElectionFiles = snapshotFiles(root.resolve("all"));
            assertReplyCode(
                    ErrorCode.FENCED_EPOCH,
                    idempotentRequest(cluster, 2, tp, 0, Acks.LEADER, 5_000, 101, 0, 0, batch));
            assertUnchanged(cluster, tp, root.resolve("all"), afterElectionLeo, afterElectionFiles);

            assertReplyCode(
                    ErrorCode.NOT_ENOUGH_REPLICAS,
                    idempotentRequest(cluster, 2, tp, 1, Acks.ALL, 5_000, 101, 0, 0, batch));
            assertUnchanged(cluster, tp, root.resolve("all"), afterElectionLeo, afterElectionFiles);
        }
    }

    @Test
    @DisplayName("Lost API 11 reply retries on a new leader and survives a real broker disk reopen")
    void lostResponseRetryAndDiskReopenPreserveOneDurableBatch() throws Exception {
        TopicPartition tp = new TopicPartition("step30-reopen", 0);
        List<RecordData> batch =
                List.of(
                        record("retry-0", "value-zero", 1_001),
                        record("retry-1", "value-one", 1_002),
                        record("retry-2", "value-two", 1_003));
        Path clusterRoot = root.resolve("reopen");

        try (ClusterHarness cluster = cluster(clusterRoot, tp.topic(), 1);
                FaultProxy proxy = new FaultProxy(cluster.endpoint(1))) {
            proxy.start();
            proxy.dropNextReply(Api.IDEMPOTENT_PRODUCE);
            RpcClientFactory clients =
                    endpoint ->
                            new RpcClient(
                                    endpoint.equals(cluster.endpoint(1))
                                            ? proxy.endpoint()
                                            : endpoint,
                                    5_000);
            try (MetadataRouter router =
                            new MetadataRouter(
                                    ClusterHarness.BROKER_IDS.stream()
                                            .map(cluster::endpoint)
                                            .toList(),
                                    clients);
                    IdempotentProducer producer = new IdempotentProducer(router, 7, 0)) {
                CourseException unknownResult =
                        assertThrows(
                                CourseException.class,
                                () -> producer.send(tp, batch, Acks.LEADER, 5_000));
                assertEquals(ErrorCode.REQUEST_TIMEOUT, unknownResult.code());
                assertEquals(1, proxy.forwarded(Api.IDEMPOTENT_PRODUCE));
                assertEquals(1, proxy.dropped(Api.IDEMPOTENT_PRODUCE));
                assertEquals(0, proxy.rejected(Api.IDEMPOTENT_PRODUCE));
                assertEquals(Map.of(1, 3L, 2, 0L, 3, 0L), leos(cluster, tp));

                LabSupport.replicateAndReport(cluster, tp);
                assertEquals(3, cluster.snapshot(tp).highWatermark());
                assertEquals(Map.of(1, 3L, 2, 3L, 3, 3L), leos(cluster, tp));

                cluster.stopBroker(1);
                assertEquals(2, cluster.elect(tp).leaderId());
                assertEquals(1, cluster.snapshot(tp).epoch());
                assertEquals(
                        new ProduceReceipt(tp, 0, 3),
                        producer.retry(tp),
                        "the explicit retry must retain the unresolved identity and original range");
                assertEquals(1, proxy.forwarded(Api.IDEMPOTENT_PRODUCE));

                List<LogRecord> expected = expectedStamped(0, 7, 0, 0, batch);
                for (int brokerId : ClusterHarness.BROKER_IDS)
                    assertEquals(expected, diskRecords(clusterRoot, brokerId, tp));
                Map<Integer, Long> beforeReopenLeo = leos(cluster, tp);
                PartitionLog previousLog = cluster.partitionLog(2, tp);
                ReplicatedPartition previousBackend = cluster.replicatedPartition(2, tp);
                cluster.reopenBrokerFromDisk(2);
                assertNotSame(previousLog, cluster.partitionLog(2, tp));
                assertNotSame(previousBackend, cluster.replicatedPartition(2, tp));
                assertEquals(beforeReopenLeo, leos(cluster, tp));

                Map<String, String> beforeDiskRetry = snapshotFiles(clusterRoot);
                Map<Integer, Long> beforeDiskRetryLeo = leos(cluster, tp);
                Messages.Reply diskRetry =
                        idempotentRequest(cluster, 2, tp, 1, Acks.LEADER, 5_000, 7, 0, 0, batch);
                assertEquals(ErrorCode.NONE, diskRetry.error());
                assertEquals(new AppendResult(0, 3), produceResult(diskRetry));
                assertEquals(beforeDiskRetryLeo, leos(cluster, tp));
                assertEquals(beforeDiskRetry, snapshotFiles(clusterRoot));
                for (int brokerId : ClusterHarness.BROKER_IDS)
                    assertEquals(expected, diskRecords(clusterRoot, brokerId, tp));

                try (RpcClient client = cluster.client(2, 5_000)) {
                    Messages.Reply fetchReply =
                            client.call(
                                    new Messages.FetchRequest(
                                            tp,
                                            1,
                                            0,
                                            10,
                                            MessageCodec.MAX_FETCH_BYTE_BUDGET,
                                            null));
                    assertEquals(ErrorCode.NONE, fetchReply.error());
                    assertEquals(
                            expected,
                            assertInstanceOf(Messages.FetchBody.class, fetchReply.body())
                                    .records());
                }
            }
        }
    }

    @Test
    @DisplayName("Bounded TCP replica fetches preserve stamps across segments and disk reopen")
    void boundedReplicaFetchAndDiskReopenPreserveStampedSegments() throws Exception {
        TopicPartition tp = new TopicPartition("step30-segments", 0);
        byte[] firstValue = new byte[600_000];
        byte[] secondValue = new byte[600_000];
        java.util.Arrays.fill(firstValue, (byte) 0x31);
        java.util.Arrays.fill(secondValue, (byte) 0x52);
        List<RecordData> firstBatch = List.of(new RecordData(null, firstValue, 1_051));
        List<RecordData> secondBatch = List.of(new RecordData(null, secondValue, 1_052));
        Path clusterRoot = root.resolve("segments");

        try (ClusterHarness cluster = cluster(clusterRoot, tp.topic(), 1);
                MetadataRouter router = router(cluster);
                IdempotentProducer producer = new IdempotentProducer(router, 33, 0)) {
            assertEquals(
                    new ProduceReceipt(tp, 0, 1),
                    producer.send(tp, firstBatch, Acks.LEADER, 5_000));
            assertEquals(
                    new ProduceReceipt(tp, 1, 2),
                    producer.send(tp, secondBatch, Acks.LEADER, 5_000));
            assertTrue(Files.exists(logFile(clusterRoot, 1, tp, 1)));

            for (int brokerId : List.of(2, 3)) {
                assertEquals(1, cluster.replicateOnce(brokerId, tp, 10, 700_000));
                assertEquals(1, cluster.partitionLog(brokerId, tp).logEndOffset());
                cluster.tracker(tp)
                        .report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
                assertEquals(1, cluster.replicateOnce(brokerId, tp, 10, 700_000));
                assertEquals(2, cluster.partitionLog(brokerId, tp).logEndOffset());
                cluster.tracker(tp)
                        .report(brokerId, 0, cluster.partitionLog(brokerId, tp).logEndOffset());
            }
            assertEquals(2, cluster.snapshot(tp).highWatermark());

            List<LogRecord> expected = new ArrayList<>();
            expected.addAll(expectedStamped(0, 33, 0, 0, firstBatch));
            expected.addAll(expectedStamped(1, 33, 0, 1, secondBatch));
            for (int brokerId : ClusterHarness.BROKER_IDS) {
                assertTrue(Files.exists(logFile(clusterRoot, brokerId, tp, 0)));
                assertTrue(Files.exists(logFile(clusterRoot, brokerId, tp, 1)));
                assertEquals(
                        List.copyOf(expected),
                        diskRecordsAcrossSegments(clusterRoot, brokerId, tp));
            }

            PartitionLog previousLog = cluster.partitionLog(2, tp);
            cluster.reopenBrokerFromDisk(2);
            assertNotSame(previousLog, cluster.partitionLog(2, tp));
            assertEquals(2, cluster.partitionLog(2, tp).logEndOffset());
            assertEquals(List.copyOf(expected), diskRecordsAcrossSegments(clusterRoot, 2, tp));
        }
    }

    @Test
    @DisplayName(
            "Reconciliation treats stamps as identity inside HW and repairs divergent tails above HW")
    void reconciliationComparesProducerStampsAtCommittedAndUncommittedOffsets() {
        TopicPartition tp = new TopicPartition("step30-reconcile", 0);
        RecordData samePayload = record("same-key", "same-payload", 1_101);
        List<RecordData> firstBatch = List.of(samePayload);
        Path clusterRoot = root.resolve("reconcile");

        try (ClusterHarness cluster = cluster(clusterRoot, tp.topic(), 1)) {
            assertEquals(
                    new AppendResult(0, 1),
                    produceResult(
                            idempotentRequest(
                                    cluster, 1, tp, 0, Acks.LEADER, 5_000, 50, 0, 0, firstBatch)));
            LabSupport.replicateAndReport(cluster, tp);
            assertEquals(1, cluster.snapshot(tp).highWatermark());

            PartitionLog follower = cluster.partitionLog(2, tp);
            follower.truncateTo(0);
            follower.append(stampedBatch(51, 0, 0, firstBatch));
            List<LogRecord> committedForgery = expectedStamped(0, 51, 0, 0, firstBatch);
            assertEquals(committedForgery, diskRecords(clusterRoot, 2, tp));
            CourseException committedConflict =
                    assertThrows(CourseException.class, () -> cluster.reconcile(2, tp));
            assertEquals(ErrorCode.CORRUPT_RECORD, committedConflict.code());
            assertEquals(committedForgery, diskRecords(clusterRoot, 2, tp));

            follower.truncateTo(0);
            follower.append(
                    expectedStamped(0, 50, 0, 0, firstBatch).stream()
                            .map(LogRecord::data)
                            .toList());
            List<RecordData> secondBatch = List.of(samePayload);
            assertEquals(
                    new AppendResult(1, 2),
                    produceResult(
                            idempotentRequest(
                                    cluster, 1, tp, 0, Acks.LEADER, 5_000, 50, 0, 1, secondBatch)));
            assertEquals(1, cluster.snapshot(tp).highWatermark());
            follower.append(stampedBatch(51, 0, 0, secondBatch));
            assertEquals(2, cluster.reconcile(2, tp));
            List<LogRecord> expected = new ArrayList<>();
            expected.addAll(expectedStamped(0, 50, 0, 0, firstBatch));
            expected.addAll(expectedStamped(1, 50, 0, 1, secondBatch));
            assertEquals(List.copyOf(expected), diskRecords(clusterRoot, 2, tp));
            assertEquals(expected, diskRecords(clusterRoot, 1, tp));
            assertEquals(1, cluster.snapshot(tp).highWatermark());
        }
    }

    private static ClusterHarness cluster(Path path, String topic, int partitions) {
        return cluster(path, topic, partitions, new AtomicLong(0));
    }

    private static ClusterHarness cluster(
            Path path, String topic, int partitions, AtomicLong clock) {
        ClusterHarness cluster = new ClusterHarness(path, clock::get);
        cluster.start();
        cluster.createTopic(topic, partitions, 2);
        return cluster;
    }

    private static MetadataRouter router(ClusterHarness cluster) {
        return new MetadataRouter(
                ClusterHarness.BROKER_IDS.stream().map(cluster::endpoint).toList(),
                endpoint -> new RpcClient(endpoint, 5_000));
    }

    private static Messages.IdempotentProduceRequest request(
            TopicPartition tp,
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records) {
        return new Messages.IdempotentProduceRequest(
                tp, 0, Acks.LEADER, 5_000, producerId, producerEpoch, firstSequence, records);
    }

    private static Messages.Reply idempotentRequest(
            ClusterHarness cluster,
            int brokerId,
            TopicPartition tp,
            int leaderEpoch,
            Acks acks,
            long timeoutMillis,
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records) {
        try (RpcClient client = cluster.client(brokerId, 10_000)) {
            return client.call(
                    new Messages.IdempotentProduceRequest(
                            tp,
                            leaderEpoch,
                            acks,
                            timeoutMillis,
                            producerId,
                            producerEpoch,
                            firstSequence,
                            records));
        }
    }

    private static AppendResult produceResult(Messages.Reply reply) {
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.ProduceBody.class, reply.body()).result();
    }

    private static void assertReplyCode(ErrorCode expected, Messages.Reply reply) {
        assertEquals(expected, reply.error());
        assertInstanceOf(Messages.ErrorBody.class, reply.body());
    }

    private static CourseException assertCode(ErrorCode expected, Executable action) {
        CourseException failure = assertThrows(CourseException.class, action);
        assertEquals(expected, failure.code());
        return failure;
    }

    private static void assertUnchanged(
            ClusterHarness cluster,
            TopicPartition tp,
            Path directory,
            Map<Integer, Long> expectedLeo,
            Map<String, String> expectedFiles)
            throws IOException {
        assertEquals(expectedLeo, leos(cluster, tp));
        assertEquals(expectedFiles, snapshotFiles(directory));
    }

    private static Map<Integer, Long> leos(ClusterHarness cluster, TopicPartition tp) {
        Map<Integer, Long> result = new TreeMap<>();
        for (int brokerId : ClusterHarness.BROKER_IDS)
            result.put(brokerId, cluster.partitionLog(brokerId, tp).logEndOffset());
        return Map.copyOf(result);
    }

    private static Map<String, String> snapshotFiles(Path directory) throws IOException {
        Map<String, String> result = new TreeMap<>();
        if (!Files.exists(directory)) return Map.of();
        try (var paths = Files.walk(directory)) {
            for (Path path : paths.filter(Files::isRegularFile).toList())
                result.put(
                        directory.relativize(path).toString(),
                        HexFormat.of().formatHex(Files.readAllBytes(path)));
        }
        return Map.copyOf(result);
    }

    private static Path logFile(Path root, int brokerId, TopicPartition tp) {
        return logFile(root, brokerId, tp, 0);
    }

    private static Path logFile(Path root, int brokerId, TopicPartition tp, long baseOffset) {
        return logDirectory(root, brokerId, tp)
                .resolve(String.format(Locale.ROOT, "%020d.log", baseOffset));
    }

    private static Path logDirectory(Path root, int brokerId, TopicPartition tp) {
        return root.resolve("broker-" + brokerId)
                .resolve(tp.topic())
                .resolve(Integer.toString(tp.partition()));
    }

    private static Path logFile(Path directory, long baseOffset) {
        return directory.resolve(String.format(Locale.ROOT, "%020d.log", baseOffset));
    }

    private static List<LogRecord> diskRecords(Path root, int brokerId, TopicPartition tp) {
        return RecordBytes.readRecords(logFile(root, brokerId, tp));
    }

    private static List<LogRecord> diskRecordsAcrossSegments(
            Path root, int brokerId, TopicPartition tp) throws IOException {
        Path directory = logDirectory(root, brokerId, tp);
        List<Path> segments;
        try (var paths = Files.list(directory)) {
            segments =
                    paths.filter(path -> path.getFileName().toString().endsWith(".log"))
                            .sorted()
                            .toList();
        }
        List<LogRecord> records = new ArrayList<>();
        for (Path segment : segments) records.addAll(RecordBytes.readRecords(segment));
        return List.copyOf(records);
    }

    private static List<RecordData> stampedBatch(
            long producerId, int producerEpoch, long firstSequence, List<RecordData> records) {
        return stampedSlice(
                producerId,
                producerEpoch,
                firstSequence,
                records,
                independentFingerprint(records),
                0,
                records.size());
    }

    private static List<RecordData> stampedSlice(
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records,
            byte[] hash,
            int fromIndex,
            int toIndex) {
        List<RecordData> result = new ArrayList<>(toIndex - fromIndex);
        for (int index = fromIndex; index < toIndex; index++) {
            RecordData data = records.get(index);
            result.add(
                    new RecordData(
                            data.key(),
                            data.value(),
                            data.timestamp(),
                            new ProducerStamp(
                                    producerId,
                                    producerEpoch,
                                    firstSequence,
                                    records.size(),
                                    index,
                                    hash)));
        }
        return List.copyOf(result);
    }

    private static List<LogRecord> expectedStamped(
            long firstOffset,
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records) {
        List<RecordData> stamped = stampedBatch(producerId, producerEpoch, firstSequence, records);
        List<LogRecord> result = new ArrayList<>(stamped.size());
        for (int index = 0; index < stamped.size(); index++)
            result.add(new LogRecord(firstOffset + index, stamped.get(index)));
        return List.copyOf(result);
    }

    private static byte[] independentFingerprint(List<RecordData> records) {
        try {
            MessageDigest digest = MessageDigest.getInstance("SHA-256");
            updateInt(digest, records.size());
            for (RecordData record : records) {
                updateLong(digest, record.timestamp());
                byte[] key = record.key();
                updateInt(digest, key == null ? -1 : key.length);
                if (key != null) digest.update(key);
                byte[] value = record.value();
                updateInt(digest, value.length);
                digest.update(value);
            }
            return digest.digest();
        } catch (NoSuchAlgorithmException exception) {
            throw new AssertionError("SHA-256 is required by the JDK", exception);
        }
    }

    private static void updateInt(MessageDigest digest, int value) {
        digest.update((byte) (value >>> 24));
        digest.update((byte) (value >>> 16));
        digest.update((byte) (value >>> 8));
        digest.update((byte) value);
    }

    private static void updateLong(MessageDigest digest, long value) {
        for (int shift = 56; shift >= 0; shift -= 8) digest.update((byte) (value >>> shift));
    }

    private static byte[] encoded(LogRecord record) {
        ByteBuffer encoded = RecordCodec.encode(record);
        byte[] result = new byte[encoded.remaining()];
        encoded.get(result);
        return result;
    }

    private static byte[] hashBytes() {
        byte[] result = new byte[32];
        for (int index = 0; index < result.length; index++) result[index] = (byte) (index * 7 + 3);
        return result;
    }

    private static RecordData record(String key, String value, long timestamp) {
        return new RecordData(key == null ? null : bytes(key), bytes(value), timestamp);
    }

    private static byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
