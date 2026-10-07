package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.OffsetBrokerFixture;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Arrays;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Step15Test {
    @Test
    void offsetStorePersistsNextOffsetsPerGroupAndPartitionAndAllowsRewind() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path storePath = temp.root().resolve("offsets");
            TopicPartition orders0 = new TopicPartition("orders", 0);
            TopicPartition orders1 = new TopicPartition("orders", 1);
            TopicPartition payments0 = new TopicPartition("payments", 0);
            OffsetKey worker0 = new OffsetKey("workers", orders0);
            OffsetKey otherGroup = new OffsetKey("other", orders0);
            OffsetKey worker1 = new OffsetKey("workers", orders1);
            OffsetKey otherTopic = new OffsetKey("workers", payments0);

            try (OffsetStore store = new OffsetStore(storePath)) {
                assertEquals(OptionalLong.empty(), store.fetch(worker0));
                store.commit(worker0, 3);
                store.commit(otherGroup, 0);
                store.commit(worker1, 7);
                store.commit(otherTopic, 11);
                store.commit(worker0, 1);
                assertEquals(OptionalLong.of(1), store.fetch(worker0));
                assertCode(ErrorCode.INVALID_REQUEST, () -> store.commit(worker0, -1));
                assertEquals(OptionalLong.of(1), store.fetch(worker0));
            }

            try (OffsetStore reopened = new OffsetStore(storePath)) {
                assertEquals(OptionalLong.of(1), reopened.fetch(worker0));
                assertEquals(OptionalLong.of(0), reopened.fetch(otherGroup));
                assertEquals(OptionalLong.of(7), reopened.fetch(worker1));
                assertEquals(OptionalLong.of(11), reopened.fetch(otherTopic));
            }
        }
    }

    @Test
    void rawOffsetRequestsReturnExactBodiesAndIsolateGroupTopicAndPartitionKeys() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             OffsetBrokerFixture broker = new OffsetBrokerFixture(temp.root(), 1)) {
            OffsetKey workersOrders0 = new OffsetKey("workers", new TopicPartition("orders", 0));
            OffsetKey workersOrders1 = new OffsetKey("workers", new TopicPartition("orders", 1));
            OffsetKey otherGroupOrders0 = new OffsetKey("other", new TopicPartition("orders", 0));
            OffsetKey workersPayments0 = new OffsetKey("workers", new TopicPartition("payments", 0));

            assertRawOffset(broker.endpoint(), workersOrders0, OptionalLong.empty(), 1);
            assertRawCommit(broker.endpoint(), workersOrders0, 5, 2);
            assertRawCommit(broker.endpoint(), workersOrders1, 7, 3);
            assertRawCommit(broker.endpoint(), otherGroupOrders0, 11, 4);
            assertRawCommit(broker.endpoint(), workersPayments0, 13, 5);
            assertRawCommit(broker.endpoint(), workersOrders0, 2, 6);

            Frame rejected = rawCall(broker.endpoint(), Api.COMMIT_OFFSET, 7,
                    commitPayload(workersOrders0, -1));
            assertEquals(ErrorCode.INVALID_REQUEST, rejected.error());
            assertRawOffset(broker.endpoint(), workersOrders0, OptionalLong.of(2), 8);
            assertRawOffset(broker.endpoint(), workersOrders1, OptionalLong.of(7), 9);
            assertRawOffset(broker.endpoint(), otherGroupOrders0, OptionalLong.of(11), 10);
            assertRawOffset(broker.endpoint(), workersPayments0, OptionalLong.of(13), 11);
        }
    }

    @Test
    void resumeCommitsAllPositionsAndDoesNotClampPastLogEnd() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            OffsetKey workersZero = new OffsetKey("workers", zero);
            OffsetKey workersOne = new OffsetKey("workers", one);

            try (OffsetBrokerFixture broker = new OffsetBrokerFixture(temp.root(), 1)) {
                broker.catalog().createTopic("orders", 2);
                try (RpcClient writer = broker.client()) {
                    TestSupport.append(writer, zero, TestSupport.values("p0-", 3));
                    TestSupport.append(writer, one, TestSupport.values("p1-", 1));
                }
                assertRawCommit(broker.endpoint(), workersZero, 2, 20);

                try (RpcClient client = broker.client();
                     SimpleConsumer consumer = new SimpleConsumer(client, "workers")) {
                    consumer.resume(List.of(one, zero));
                    assertEquals(2, consumer.position(zero));
                    assertEquals(0, consumer.position(one));
                    assertRawCommit(broker.endpoint(), workersOne, 5, 21);
                    assertRawOffset(broker.endpoint(), workersOne, OptionalLong.of(5), 26);
                    var polled = consumer.poll(1, 4_096);
                    assertEquals(List.of(zero), List.copyOf(polled.keySet()));
                    assertEquals(List.of(2L), polled.get(zero).stream().map(record -> record.offset()).toList());
                    assertEquals(3, consumer.position(zero));
                    assertEquals(0, consumer.position(one));
                    consumer.commitSync();
                }
            }

            try (OffsetBrokerFixture reopened = new OffsetBrokerFixture(temp.root(), 1)) {
                reopened.catalog().createTopic("orders", 2);
                assertRawOffset(reopened.endpoint(), workersZero, OptionalLong.of(3), 22);
                assertRawOffset(reopened.endpoint(), workersOne, OptionalLong.of(0), 23);
                assertRawCommit(reopened.endpoint(), workersZero, 99, 24);

                try (RpcClient client = reopened.client();
                     SimpleConsumer beyondEnd = new SimpleConsumer(client, "workers")) {
                    beyondEnd.resume(List.of(zero));
                    assertEquals(99, beyondEnd.position(zero));
                    assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> beyondEnd.poll(1, 4_096));
                    assertEquals(99, beyondEnd.position(zero));
                }
                assertRawOffset(reopened.endpoint(), workersZero, OptionalLong.of(99), 25);
            }
        }
    }

    @Test
    void offsetStoreTruncatesAnIncompleteTailAndRejectsCompleteBadCrcWithoutMutation() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            OffsetKey key = new OffsetKey("workers", new TopicPartition("orders", 0));
            Path partialRoot = temp.root().resolve("test-offset-store");
            Path partialLog = partialRoot.resolve("00000000000000000000.log");
            byte[] original;
            try (OffsetStore store = new OffsetStore(partialRoot)) {
                store.commit(key, 2);
            }
            original = Files.readAllBytes(partialLog);
            byte[] secondRecord = RecordBytes.record(1, null, MessageCodec.encodeOffsetEntry(key, 3), 0);
            byte[] incompleteTail = Arrays.copyOf(secondRecord, Integer.BYTES * 2);
            assertEquals(8, incompleteTail.length, "the damaged tail is exactly the length and CRC prefix");
            Files.write(partialLog, incompleteTail, StandardOpenOption.APPEND);

            try (OffsetStore reopened = new OffsetStore(partialRoot)) {
                assertEquals(OptionalLong.of(2), reopened.fetch(key));
            }
            assertEquals(original.length, Files.size(partialLog), "recovery must truncate the incomplete final record");
            assertArrayEquals(original, Files.readAllBytes(partialLog));

            Path corruptRoot = temp.root().resolve("corrupt-offset-store");
            Path corruptLog = corruptRoot.resolve("00000000000000000000.log");


            try (OffsetStore store = new OffsetStore(corruptRoot)) {
                store.commit(key, 2);
            }
            byte[] validSecond = RecordBytes.record(1, null, MessageCodec.encodeOffsetEntry(key, 3), 0);
            byte[] badCrcSecond = RecordBytes.withField(validSecond, 32, 1, 1, false);
            Files.write(corruptLog, badCrcSecond, StandardOpenOption.APPEND);
            byte[] corruptLogBefore = Files.readAllBytes(corruptLog);


            assertCode(ErrorCode.CORRUPT_RECORD, () -> {
                try (OffsetStore reopened = new OffsetStore(corruptRoot)) {
                    reopened.fetch(key);
                }
            });
            assertArrayEquals(corruptLogBefore, Files.readAllBytes(corruptLog),
                    "a complete record with a bad CRC must not be skipped or rewritten");
        }
    }

    @Test
    void simpleConsumerCommitSurvivesBrokerRestartAndResumeIsGroupScoped() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            TopicPartition tp = new TopicPartition("orders", 0);
            try (OffsetBrokerFixture broker = new OffsetBrokerFixture(temp.root(), 1);
                 RpcClient client = broker.client()) {
                broker.catalog().createTopic("orders", 1);
                TestSupport.append(client, tp, TestSupport.values("record-", 3));
                try (SimpleConsumer worker = new SimpleConsumer(client, "workers")) {
                    worker.assign(List.of(tp));
                    assertEquals(List.of(0L, 1L), worker.poll(2, 4_096).get(tp).stream()
                            .map(record -> record.offset()).toList());
                    assertEquals(2, worker.position(tp));
                    worker.commitSync();
                    assertRawOffset(broker.endpoint(), new OffsetKey("workers", tp), OptionalLong.of(2), 27);
                    worker.seek(tp, 0);
                    assertEquals(0, worker.position(tp));
                    worker.commitSync();
                    assertRawOffset(broker.endpoint(), new OffsetKey("workers", tp), OptionalLong.of(0), 28);
                }
            }

            try (OffsetBrokerFixture restarted = new OffsetBrokerFixture(temp.root(), 1)) {
                restarted.catalog().createTopic("orders", 1);
                assertRawOffset(restarted.endpoint(), new OffsetKey("workers", tp), OptionalLong.of(0), 29);
                try (RpcClient resumedClient = restarted.client();
                     SimpleConsumer resumed = new SimpleConsumer(resumedClient, "workers")) {
                    resumed.resume(List.of(tp));
                    assertEquals(0, resumed.position(tp), "resume restores the committed rewind after restart");
                    assertEquals(List.of(0L, 1L), resumed.poll(2, 4_096).get(tp).stream()
                            .map(record -> record.offset()).toList());
                }
                try (RpcClient independentClient = restarted.client();
                     SimpleConsumer independent = new SimpleConsumer(independentClient, "other-workers")) {
                    independent.resume(List.of(tp));
                    assertEquals(0, independent.position(tp));
                }
            }
        }
    }

    private static void assertRawCommit(Endpoint endpoint, OffsetKey key, long nextOffset,
                                        int correlationId) throws IOException {
        Frame reply = rawCall(endpoint, Api.COMMIT_OFFSET, correlationId, commitPayload(key, nextOffset));
        assertEquals(ErrorCode.NONE, reply.error());
        assertArrayEquals(new byte[0], reply.payload(), "successful COMMIT_OFFSET has an empty wire body");
        assertInstanceOf(Messages.EmptyBody.class, MessageCodec.decodeReply(reply.api(), reply.error(), reply.payload()));
    }

    private static void assertRawOffset(Endpoint endpoint, OffsetKey key, OptionalLong expected,
                                        int correlationId) throws IOException {
        Frame reply = rawCall(endpoint, Api.FETCH_OFFSET, correlationId, offsetKeyPayload(key));
        assertEquals(ErrorCode.NONE, reply.error());
        byte[] expectedWire = expected.isPresent()
                ? ByteBuffer.allocate(1 + Long.BYTES).order(ByteOrder.BIG_ENDIAN)
                        .put((byte) 1).putLong(expected.getAsLong()).array()
                : new byte[]{0};
        assertArrayEquals(expectedWire, reply.payload(), "FETCH_OFFSET must encode the exact optional next offset");
        Messages.OffsetBody body = assertInstanceOf(Messages.OffsetBody.class,
                MessageCodec.decodeReply(reply.api(), reply.error(), reply.payload()));
        assertEquals(expected, body.nextOffset());
    }

    private static Frame rawCall(Endpoint endpoint, short api, int correlationId, byte[] payload)
            throws IOException {
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), 2_000);
            socket.setSoTimeout(2_000);
            FrameCodec.write(socket.getOutputStream(), new Frame(api, correlationId, ErrorCode.NONE, payload));
            Frame reply = FrameCodec.read(socket.getInputStream());
            assertNotNull(reply, "broker closed the raw connection without a response");
            assertEquals(api, reply.api());
            assertEquals(correlationId, reply.correlationId());
            return reply;
        }
    }

    private static byte[] commitPayload(OffsetKey key, long nextOffset) {
        byte[] encodedKey = offsetKeyPayload(key);
        return ByteBuffer.allocate(encodedKey.length + Long.BYTES + 1).order(ByteOrder.BIG_ENDIAN)
                .put(encodedKey).putLong(nextOffset).put((byte) 0).array();
    }

    private static byte[] offsetKeyPayload(OffsetKey key) {
        byte[] group = key.group().getBytes(StandardCharsets.UTF_8);
        byte[] topic = key.tp().topic().getBytes(StandardCharsets.UTF_8);
        return ByteBuffer.allocate(Integer.BYTES + group.length + Integer.BYTES + topic.length + Integer.BYTES)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(group.length).put(group)
                .putInt(topic.length).put(topic)
                .putInt(key.tp().partition())
                .array();
    }
}
