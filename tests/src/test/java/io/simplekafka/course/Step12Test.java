package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.RecordBytes;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.HexFormat;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.record;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;

class Step12Test {
    private static final byte[] METADATA_T_BODY = hex("00000001 74");
    private static final byte[] PRODUCE_PREFIX = hex("00000001 74 00000000 00000000 0001 00000000000003e8");
    private static final byte[] PRODUCE_T_BODY = hex(
            "00000001 74 00000000 00000000 0001 00000000000003e8 " +
                    "00000001 ffffffff 00000001 76 0000000000000000");
    private static final byte[] FETCH_PREFIX = hex(
            "00000001 74 00000000 00000000 0000000000000000 0000000a 00001000");
    private static final byte[] FETCH_T_BODY = hex(
            "00000001 74 00000000 00000000 0000000000000000 0000000a 00001000 00");

    @Test
    void servesPreciseMetadataRecordsAndFailuresWithoutMutatingTheLog() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient client = new RpcClient(broker.endpoint(), 2_000)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            Messages.Reply metadataReply = client.call(new Messages.MetadataRequest("orders"));
            assertEquals(ErrorCode.NONE, metadataReply.error());
            Messages.MetadataBody metadata = assertInstanceOf(Messages.MetadataBody.class, metadataReply.body());
            assertEquals(List.of(new PartitionMetadata(tp, 1, 0, List.of(1), List.of(1), broker.endpoint())),
                    metadata.partitions());

            Messages.Reply produced = client.call(new Messages.ProduceRequest(tp, 0, Acks.LEADER, 1_000,
                    List.of(record("k", "v0", 10), record("k", "v1", 11), record(null, "v2", 12))));
            assertEquals(ErrorCode.NONE, produced.error());
            assertEquals(new AppendResult(0, 3),
                    assertInstanceOf(Messages.ProduceBody.class, produced.body()).result());

            Messages.FetchBody fetched = TestSupport.fetch(client, tp, 0, 10, 4_096);
            assertEquals(List.of(
                    new LogRecord(0, record("k", "v0", 10)),
                    new LogRecord(1, record("k", "v1", 11)),
                    new LogRecord(2, record(null, "v2", 12))), fetched.records());
            assertFetchMetadata(fetched, 0, 3, 3);

            Messages.FetchBody atEnd = TestSupport.fetch(client, tp, 3, 10, 4_096);
            assertEquals(List.of(), atEnd.records(), "fetch at LEO is an empty successful response");
            assertFetchMetadata(atEnd, 0, 3, 3);

            Messages.FetchBody firstTooLarge = TestSupport.fetch(client, tp, 0, 10, 1);
            assertEquals(List.of(), firstTooLarge.records(),
                    "the first record is not returned when it exceeds the entire byte budget");
            assertFetchMetadata(firstTooLarge, 0, 3, 3);
            List<LogRecord> firstTwoRecords = List.of(
                    new LogRecord(0, record("k", "v0", 10)),
                    new LogRecord(1, record("k", "v1", 11)));
            Messages.FetchBody recordLimited = TestSupport.fetch(client, tp, 0, 2, 4_096);
            assertEquals(firstTwoRecords, recordLimited.records(),
                    "maxRecords=2 returns exactly the first two complete records");
            assertFetchMetadata(recordLimited, 0, 3, 3);

            int oneRecordBytes = RecordBytes.record(0, TestSupport.utf8("k"),
                    TestSupport.utf8("v0"), 10).length;
            Messages.FetchBody oneByteShort = TestSupport.fetch(client, tp, 0, 10,
                    oneRecordBytes * 2 - 1);
            assertEquals(List.of(new LogRecord(0, record("k", "v0", 10))), oneByteShort.records(),
                    "one byte less than two complete records returns only the first");
            assertFetchMetadata(oneByteShort, 0, 3, 3);

            Messages.FetchBody exactByteBudget = TestSupport.fetch(client, tp, 0, 10,
                    oneRecordBytes * 2);
            assertEquals(firstTwoRecords, exactByteBudget.records(),
                    "an exact two-record byte budget includes both records and no third");
            assertFetchMetadata(exactByteBudget, 0, 3, 3);

            Map<String, String> unchanged = snapshotFiles(temp.root());
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.MetadataRequest("missing"), ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.ProduceRequest(new TopicPartition("missing", 0), 0, Acks.LEADER, 1_000,
                            List.of(record(null, "must-not-append", 13))), ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.FetchRequest(new TopicPartition("missing", 0), 0, 0, 1, 256, null),
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.ProduceRequest(tp, -1, Acks.LEADER, 1_000,
                            List.of(record(null, "must-not-append", 13))), ErrorCode.FENCED_EPOCH);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.FetchRequest(tp, -1, 0, 1, 256, null), ErrorCode.FENCED_EPOCH);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.ProduceRequest(tp, 0, Acks.LEADER, 1_000, List.of()), ErrorCode.INVALID_REQUEST);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.ProduceRequest(tp, 0, Acks.LEADER, -1,
                            List.of(record(null, "must-not-append", 13))), ErrorCode.INVALID_REQUEST);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.FetchRequest(tp, 0, 0, 0, 256, null), ErrorCode.INVALID_REQUEST);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.FetchRequest(tp, 0, 0, 1, 0, null), ErrorCode.INVALID_REQUEST);
            assertRpcErrorWithoutMutation(client, temp.root(), unchanged,
                    new Messages.FetchRequest(tp, 0, 4, 1, 256, null), ErrorCode.OFFSET_OUT_OF_RANGE);

            Messages.Reply appended = client.call(new Messages.ProduceRequest(tp, 0, Acks.LEADER, 1_000,
                    List.of(record(null, "after-errors", 14))));
            assertEquals(ErrorCode.NONE, appended.error());
            assertEquals(new AppendResult(3, 4),
                    assertInstanceOf(Messages.ProduceBody.class, appended.body()).result());
            assertEquals(List.of(
                    new LogRecord(0, record("k", "v0", 10)),
                    new LogRecord(1, record("k", "v1", 11)),
                    new LogRecord(2, record(null, "v2", 12)),
                    new LogRecord(3, record(null, "after-errors", 14))),
                    TestSupport.fetch(client, tp, 0, 10, 4_096).records());
        }
    }

    @Test
    void boundsLargeFetchRepliesAndPreservesRecordContinuation() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 1);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000)) {
            TopicPartition tp = new TopicPartition("orders", 0);
            byte[] value = new byte[1_048_548];
            for (int offset = 0; offset < 9; offset++) {
                assertEquals(new AppendResult(offset, offset + 1),
                        TestSupport.append(client, tp, List.of(new RecordData(null, value, offset))));
            }

            Messages.FetchBody firstPage = TestSupport.fetch(client, tp, 0, 9, 10_000_000);
            assertEquals(largeRecords(value, 0, 7), firstPage.records());
            assertFetchMetadata(firstPage, 0, 9, 9);
            assertEquals(7_340_036, MessageCodec.encodeReply(firstPage).length);

            Messages.FetchBody secondPage = TestSupport.fetch(client, tp, 7, 9, Integer.MAX_VALUE);
            assertEquals(largeRecords(value, 7, 9), secondPage.records());
            assertFetchMetadata(secondPage, 0, 9, 9);
        }
    }

    @Test
    void returnsAnErrorReplyWhenAHandlerProducesAnOversizedFetchBody() {
        byte[] value = new byte[1_048_548];
        List<LogRecord> records = largeRecords(value, 0, 9);
        try (BrokerServer server = new BrokerServer("127.0.0.1", 0, request ->
                Messages.Reply.success(new Messages.FetchBody(records, 0, 9, 9, 0)));
             RpcClient client = new RpcClient(server.start(), 3_000)) {
            Messages.Reply reply = client.call(new Messages.FetchRequest(
                    new TopicPartition("orders", 0), 0, 0, 1, 1, null));
            assertEquals(ErrorCode.INVALID_REQUEST, reply.error());
            assertEquals(new Messages.ErrorBody("broker reply could not be encoded"), reply.body());
        }
    }

    @Test
    void messageCodecMatchesIndependentManualRequestBodiesInBothDirections() {
        Messages.MetadataRequest metadata = new Messages.MetadataRequest("t");
        assertArrayEquals(METADATA_T_BODY, MessageCodec.encodeRequest(metadata));
        assertEquals(metadata, MessageCodec.decodeRequest(Api.METADATA, METADATA_T_BODY));

        Messages.ProduceRequest produce = new Messages.ProduceRequest(new TopicPartition("t", 0), 0,
                Acks.LEADER, 1_000, List.of(record(null, "v", 0)));
        assertArrayEquals(PRODUCE_T_BODY, MessageCodec.encodeRequest(produce));
        assertEquals(produce, MessageCodec.decodeRequest(Api.PRODUCE, PRODUCE_T_BODY));

        Messages.FetchRequest fetch = new Messages.FetchRequest(new TopicPartition("t", 0), 0, 0, 10, 4_096, null);
        assertArrayEquals(FETCH_T_BODY, MessageCodec.encodeRequest(fetch));
        assertEquals(fetch, MessageCodec.decodeRequest(Api.FETCH, FETCH_T_BODY));

        for (InvalidBody invalid : invalidBodies()) {
            assertCode(ErrorCode.INVALID_REQUEST,
                    () -> MessageCodec.decodeRequest(invalid.api(), invalid.body()));
        }
    }

    @Test
    void rawTcpRequestsAndMalformedBodiesStayIsolatedToTheirConnections() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "t", 1);
             RpcClient client = new RpcClient(broker.endpoint(), 2_000)) {
            TopicPartition tp = new TopicPartition("t", 0);
            assertEquals(new AppendResult(0, 1),
                    TestSupport.append(client, tp, List.of(record("seed", "before-errors", 7))));

            Map<String, String> unchanged = snapshotFiles(temp.root());
            try (Socket socket = connect(broker.endpoint().host(), broker.endpoint().port())) {
                Frame metadataFrame = rawRequest(socket, Api.METADATA, 7, 0, METADATA_T_BODY);
                assertEquals(Api.METADATA, metadataFrame.api());
                assertEquals(7, metadataFrame.correlationId());
                assertEquals(ErrorCode.NONE, metadataFrame.error());
                Messages.MetadataBody metadata = assertInstanceOf(Messages.MetadataBody.class,
                        MessageCodec.decodeReply(metadataFrame.api(), metadataFrame.error(), metadataFrame.payload()));
                assertEquals(List.of(new PartitionMetadata(tp, 1, 0, List.of(1), List.of(1), broker.endpoint())),
                        metadata.partitions());

                Frame fetchFrame = rawRequest(socket, Api.FETCH, 9, 0, FETCH_T_BODY);
                assertEquals(Api.FETCH, fetchFrame.api());
                assertEquals(9, fetchFrame.correlationId());
                assertEquals(ErrorCode.NONE, fetchFrame.error());
                Messages.FetchBody fetch = assertInstanceOf(Messages.FetchBody.class,
                        MessageCodec.decodeReply(fetchFrame.api(), fetchFrame.error(), fetchFrame.payload()));
                assertEquals(List.of(new LogRecord(0, record("seed", "before-errors", 7))), fetch.records());
                assertFetchMetadata(fetch, 0, 1, 1);

                Frame nonzeroRequestError = rawRequest(socket, Api.PRODUCE, 11, 8, PRODUCE_T_BODY);
                assertEquals(Api.PRODUCE, nonzeroRequestError.api());
                assertEquals(11, nonzeroRequestError.correlationId());
                assertEquals(ErrorCode.INVALID_REQUEST, nonzeroRequestError.error());
                assertEquals(unchanged, snapshotFiles(temp.root()), "nonzero request frame error appended data");

                Frame trailingProduce = rawRequest(socket, Api.PRODUCE, 12, 0,
                        concat(PRODUCE_T_BODY, hex("00000000")));
                assertEquals(Api.PRODUCE, trailingProduce.api());
                assertEquals(12, trailingProduce.correlationId());
                assertEquals(ErrorCode.INVALID_REQUEST, trailingProduce.error());
                assertEquals(unchanged, snapshotFiles(temp.root()), "trailing request bytes appended data");

                int correlation = 20;
                for (InvalidBody invalid : invalidBodies()) {
                    Frame response = rawRequest(socket, invalid.api(), correlation++, 0, invalid.body());
                    assertEquals(invalid.api(), response.api(), invalid.name());
                    assertEquals(ErrorCode.INVALID_REQUEST, response.error(), invalid.name());
                    assertEquals(unchanged, snapshotFiles(temp.root()), invalid.name() + " changed disk state");
                }
            }

            for (byte[] malformedHeader : List.of(hex("00000009"), hex("0000000d00"))) {
                assertMalformedConnectionCloses(broker.endpoint().host(), broker.endpoint().port(), malformedHeader);
                assertEquals(unchanged, snapshotFiles(temp.root()), "malformed frame changed disk state");
                assertMetadataStillAvailable(broker.endpoint());
            }

            Messages.Reply appended = client.call(new Messages.ProduceRequest(tp, 0, Acks.LEADER, 1_000,
                    List.of(record(null, "after-raw-errors", 8))));
            assertEquals(ErrorCode.NONE, appended.error());
            assertEquals(new AppendResult(1, 2),
                    assertInstanceOf(Messages.ProduceBody.class, appended.body()).result());
            assertEquals(List.of(
                    new LogRecord(0, record("seed", "before-errors", 7)),
                    new LogRecord(1, record(null, "after-raw-errors", 8))),
                    TestSupport.fetch(client, tp, 0, 10, 4_096).records());
        }
    }

    private static List<LogRecord> largeRecords(byte[] value, int firstOffset, int nextOffset) {
        List<LogRecord> records = new ArrayList<>(nextOffset - firstOffset);
        for (int offset = firstOffset; offset < nextOffset; offset++)
            records.add(new LogRecord(offset, new RecordData(null, value, offset)));
        return List.copyOf(records);
    }

    private static List<InvalidBody> invalidBodies() {
        return List.of(
                new InvalidBody(Api.METADATA, "negative topic string length", hex("ffffffff")),
                new InvalidBody(Api.METADATA, "truncated topic string", hex("00000002 74")),
                new InvalidBody(Api.PRODUCE, "negative produce record count",
                        concat(PRODUCE_PREFIX, hex("ffffffff"))),
                new InvalidBody(Api.PRODUCE, "huge produce record count",
                        concat(PRODUCE_PREFIX, hex("7fffffff"))),
                new InvalidBody(Api.PRODUCE, "key length -2",
                        concat(PRODUCE_PREFIX, hex("00000001 fffffffe"))),
                new InvalidBody(Api.PRODUCE, "value length -1",
                        concat(PRODUCE_PREFIX, hex("00000001 ffffffff ffffffff"))),
                new InvalidBody(Api.PRODUCE, "value length Integer.MAX_VALUE",
                        concat(PRODUCE_PREFIX, hex("00000001 ffffffff 7fffffff"))),
                new InvalidBody(Api.PRODUCE, "truncated value and timestamp",
                        concat(PRODUCE_PREFIX, hex("00000001 ffffffff 00000002 76"))),
                new InvalidBody(Api.FETCH, "optional token boolean 2",
                        concat(FETCH_PREFIX, hex("02"))));
    }

    private static void assertFetchMetadata(Messages.FetchBody body, long start, long end, long highWatermark) {
        assertEquals(start, body.logStartOffset());
        assertEquals(end, body.logEndOffset());
        assertEquals(highWatermark, body.highWatermark());
        assertEquals(0, body.epoch());
    }

    private static void assertRpcErrorWithoutMutation(RpcClient client, Path root, Map<String, String> before,
                                                       Messages.Request request, ErrorCode expected) throws IOException {
        assertEquals(expected, client.call(request).error(), request.toString());
        assertEquals(before, snapshotFiles(root), request + " changed disk state");
    }

    private static void assertMetadataStillAvailable(io.simplekafka.model.Endpoint endpoint) {
        try (RpcClient client = new RpcClient(endpoint, 2_000)) {
            Messages.Reply reply = client.call(new Messages.MetadataRequest("t"));
            assertEquals(ErrorCode.NONE, reply.error());
            Messages.MetadataBody metadata = assertInstanceOf(Messages.MetadataBody.class, reply.body());
            assertEquals(List.of(new TopicPartition("t", 0)),
                    metadata.partitions().stream().map(PartitionMetadata::tp).toList());
        }
    }

    private static Socket connect(String host, int port) throws IOException {
        Socket socket = new Socket();
        socket.connect(new InetSocketAddress(host, port), 2_000);
        socket.setSoTimeout(2_000);
        return socket;
    }

    private static Frame rawRequest(Socket socket, short api, int correlation, int error, byte[] body)
            throws IOException {
        socket.getOutputStream().write(rawFrame(api, correlation, error, body));
        socket.getOutputStream().flush();
        Frame response = FrameCodec.read(socket.getInputStream());
        assertNotNull(response, "broker closed without a response for API " + api);
        return response;
    }

    private static byte[] rawFrame(short api, int correlation, int error, byte[] body) {
        return ByteBuffer.allocate(Integer.BYTES + 10 + body.length).order(ByteOrder.BIG_ENDIAN)
                .putInt(10 + body.length)
                .putShort((short) 1)
                .putShort(api)
                .putInt(correlation)
                .putShort((short) error)
                .put(body)
                .array();
    }

    private static void assertMalformedConnectionCloses(String host, int port, byte[] malformedHeader)
            throws IOException {
        try (Socket socket = connect(host, port)) {
            socket.getOutputStream().write(malformedHeader);
            socket.getOutputStream().flush();
            socket.shutdownOutput();
            assertEquals(-1, socket.getInputStream().read(), "malformed frame connection should close without reply");
        }
    }

    private static Map<String, String> snapshotFiles(Path root) throws IOException {
        Map<String, String> snapshot = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted().toList()) {
                if (path.equals(root)) continue;
                String relative = root.relativize(path).toString();
                if (Files.isDirectory(path)) snapshot.put(relative, "<directory>");
                else if (Files.isRegularFile(path)) snapshot.put(relative, HexFormat.of().formatHex(Files.readAllBytes(path)));
                else snapshot.put(relative, "<other>");
            }
        }
        return Map.copyOf(snapshot);
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = new byte[first.length + second.length];
        System.arraycopy(first, 0, joined, 0, first.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }

    private static byte[] hex(String value) {
        return HexFormat.of().parseHex(value.replace(" ", ""));
    }

    private record InvalidBody(short api, String name, byte[] body) {}
}
