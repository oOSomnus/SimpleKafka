package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step14Test {
    @Test
    void pollsInPartitionOrderWithinRoundBudgetsAndPreservesManualPositions() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 2);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleConsumer consumer = new SimpleConsumer(client, "manual-group")) {
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            TestSupport.append(client, zero, List.of(emptyRecord(), emptyRecord()));
            TestSupport.append(client, one, List.of(emptyRecord(), emptyRecord()));

            consumer.assign(List.of());
            assertEquals(Map.of(), consumer.poll(4, 64), "an empty assignment performs no fetch");
            consumer.assign(List.of(one, zero, one));
            assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(0, 64));
            assertEquals(0, consumer.position(zero));
            assertEquals(0, consumer.position(one));

            Map<TopicPartition, List<LogRecord>> first = consumer.poll(4, 64);
            assertEquals(List.of(zero), List.copyOf(first.keySet()));
            assertEquals(List.of(0L, 1L), offsets(first.get(zero)));
            assertEquals(2, consumer.position(zero));
            assertEquals(0, consumer.position(one));

            consumer.seek(zero, 0);
            consumer.seek(one, 0);
            Map<TopicPartition, List<LogRecord>> byteLimited = consumer.poll(4, 63);
            assertEquals(List.of(zero), List.copyOf(byteLimited.keySet()));
            assertEquals(List.of(0L), offsets(byteLimited.get(zero)),
                    "a 31-byte remainder cannot return the next complete 32-byte record");
            assertEquals(1, consumer.position(zero));
            assertEquals(0, consumer.position(one), "an unfittable record does not advance its partition");

            consumer.seek(zero, 0);
            consumer.seek(one, 0);
            Map<TopicPartition, List<LogRecord>> recordLimited = consumer.poll(1, 64);
            assertEquals(List.of(zero), List.copyOf(recordLimited.keySet()),
                    "maxRecords is shared across the ordered partition scan");
            assertEquals(List.of(0L), offsets(recordLimited.get(zero)));
            assertEquals(1, consumer.position(zero));
            assertEquals(0, consumer.position(one));

            consumer.seek(zero, 2);
            consumer.seek(one, 1);
            consumer.assign(List.of(zero));
            assertEquals(2, consumer.position(zero), "a retained partition keeps its existing position");
            assertCode(ErrorCode.NOT_ASSIGNED, () -> consumer.position(one));
            consumer.assign(List.of(zero, one));
            assertEquals(2, consumer.position(zero));
            assertEquals(0, consumer.position(one), "a newly assigned partition starts at zero");

            assertCode(ErrorCode.NOT_ASSIGNED,
                    () -> consumer.seek(new TopicPartition("orders", 2), 0));
            assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> consumer.seek(one, -1));
            assertEquals(2, consumer.position(zero));
            assertEquals(0, consumer.position(one), "failed seeks do not alter assigned positions");

            consumer.seek(zero, 2);
            consumer.seek(one, 2);
            assertEquals(Map.of(), consumer.poll(4, 64), "seeking to each log end is an empty read");
            assertEquals(2, consumer.position(zero));
            assertEquals(2, consumer.position(one));

            consumer.assign(List.of());
            assertEquals(Map.of(), consumer.poll(4, 64));
            assertCode(ErrorCode.NOT_ASSIGNED, () -> consumer.position(zero));
        }
    }

    @Test
    void spendsTheThreeRecordBudgetOnTheFirstPartitionAndRejectsInvalidLimitsWithoutAdvancing() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 2);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleConsumer consumer = new SimpleConsumer(client, "budget-group")) {
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            TestSupport.append(client, zero, List.of(emptyRecord(), emptyRecord(), emptyRecord()));
            TestSupport.append(client, one, List.of(emptyRecord()));
            consumer.assign(List.of(one, zero));

            Map<TopicPartition, List<LogRecord>> records = consumer.poll(3, 96);
            assertEquals(List.of(zero), List.copyOf(records.keySet()),
                    "a three-record limit is consumed by the first partition's three records");
            assertEquals(List.of(0L, 1L, 2L), offsets(records.get(zero)));
            assertEquals(3, consumer.position(zero));
            assertEquals(0, consumer.position(one));

            for (int maxRecords : List.of(0, -1)) {
                assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(maxRecords, 96));
                assertEquals(3, consumer.position(zero));
                assertEquals(0, consumer.position(one));
            }
            for (int maxBytes : List.of(0, -1)) {
                assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(3, maxBytes));
                assertEquals(3, consumer.position(zero));
                assertEquals(0, consumer.position(one));
            }
        }
    }

    @Test
    void leavesFailedPartitionPositionUnchangedForFetchErrorsAndMalformedSuccessReplies() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             ScriptedFetchBroker broker = new ScriptedFetchBroker(temp.root());
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleConsumer consumer = new SimpleConsumer(client, "scripted-group")) {
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            consumer.assign(List.of(zero, one));

            broker.scriptFetch(request -> request.tp().equals(zero)
                    ? fetchReply(List.of(record(0)), 1)
                    : Messages.Reply.failure(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "scripted unknown partition"));
            assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, () -> consumer.poll(2, 64));
            assertEquals(1, consumer.position(zero), "an earlier successful partition may remain advanced");
            assertEquals(0, consumer.position(one), "the partition whose fetch failed remains unchanged");

            consumer.assign(List.of(zero));
            consumer.seek(zero, 0);
            broker.scriptFetch(request -> fetchReply(List.of(record(0), record(2)), 3));
            assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(2, 64));
            assertEquals(0, consumer.position(zero), "noncontiguous response offsets are rejected without advance");

            broker.scriptFetch(request -> fetchReply(List.of(record(0), record(1)), 2));
            assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(1, 64));
            assertEquals(0, consumer.position(zero), "a response exceeding maxRecords is rejected without advance");

            broker.scriptFetch(request -> fetchReply(List.of(record(0)), 1));
            assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(1, 31));
            assertEquals(0, consumer.position(zero), "a response exceeding maxBytes is rejected without advance");
        }
    }

    private static RecordData emptyRecord() {
        return new RecordData(null, new byte[0], 0);
    }

    private static LogRecord record(long offset) {
        return new LogRecord(offset, emptyRecord());
    }

    private static List<Long> offsets(List<LogRecord> records) {
        return records.stream().map(LogRecord::offset).toList();
    }

    private static Messages.Reply fetchReply(List<LogRecord> records, long logEndOffset) {
        return Messages.Reply.success(new Messages.FetchBody(records, 0, logEndOffset, logEndOffset, 0));
    }

    private static final class ScriptedFetchBroker implements AutoCloseable {
        private final AtomicReference<BrokerHandler> handler = new AtomicReference<>();
        private final AtomicReference<Function<Messages.FetchRequest, Messages.Reply>> fetchScript =
                new AtomicReference<>();
        private final BrokerServer server;
        private Endpoint endpoint;
        private PartitionCatalog catalog;

        private ScriptedFetchBroker(java.nio.file.Path root) {
            server = new BrokerServer("127.0.0.1", 0, this::dispatch);
            try {
                endpoint = server.start();
                catalog = new PartitionCatalog(root, 1, endpoint);
                catalog.createTopic("orders", 2);
                handler.set(new BrokerHandler(catalog, null, null, null));
            } catch (RuntimeException failure) {
                close();
                throw failure;
            }
        }

        private Messages.Reply dispatch(Messages.Request request) {
            if (request instanceof Messages.FetchRequest fetch) {
                Function<Messages.FetchRequest, Messages.Reply> script = fetchScript.get();
                if (script == null)
                    return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "fetch script is not configured");
                return script.apply(fetch);
            }
            BrokerHandler current = handler.get();
            if (current == null)
                return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "broker is not ready");
            return current.handle(request);
        }

        private Endpoint endpoint() {
            return endpoint;
        }

        private void scriptFetch(Function<Messages.FetchRequest, Messages.Reply> script) {
            fetchScript.set(script);
        }

        @Override
        public void close() {
            server.close();
            if (catalog != null) catalog.close();
        }
    }
}
