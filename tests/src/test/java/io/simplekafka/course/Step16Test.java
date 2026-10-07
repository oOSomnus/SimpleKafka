package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.ProcessingLoop;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.OffsetBrokerFixture;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.util.ArrayList;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
class Step16Test {
    @Test
    void laterPartitionFailureLeavesBothOffsetsUncommittedAndReplaysEarlierSideEffects() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             OffsetBrokerFixture broker = new OffsetBrokerFixture(temp.root(), 1)) {
            broker.catalog().createTopic("orders", 2);
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            try (RpcClient writer = broker.client()) {
                TestSupport.append(writer, zero, TestSupport.values("p0-", 2));
                TestSupport.append(writer, one, TestSupport.values("p1-", 2));
            }

            List<String> firstAttempt = new ArrayList<>();
            IllegalStateException processingFailure =
                    new IllegalStateException("processor failed at partition 1 offset 0");
            try (RpcClient firstClient = broker.client();
                 SimpleConsumer consumer = new SimpleConsumer(firstClient, "workers")) {
                consumer.assign(List.of(one, zero));
                ProcessingLoop loop = new ProcessingLoop(consumer, (tp, record) -> {
                    firstAttempt.add(delivery(tp, record));
                    if (tp.equals(one) && record.offset() == 0) throw processingFailure;
                });
                assertSame(processingFailure, assertThrows(IllegalStateException.class,
                        () -> loop.runOnce(4, 8_192)));
            }
            assertEquals(List.of("orders:0:0", "orders:0:1", "orders:1:0"), firstAttempt,
                    "partition zero side effects happen before the partition-one failure");

            try (RpcClient offsets = broker.client()) {
                assertEquals(OptionalLong.empty(), fetchOffset(offsets, "workers", zero));
                assertEquals(OptionalLong.empty(), fetchOffset(offsets, "workers", one));
            }

            List<String> retryAttempt = new ArrayList<>();
            try (RpcClient retryClient = broker.client();
                 SimpleConsumer rebuilt = new SimpleConsumer(retryClient, "workers")) {
                rebuilt.resume(List.of(one, zero));
                assertEquals(0, rebuilt.position(zero));
                assertEquals(0, rebuilt.position(one));
                ProcessingLoop retry = new ProcessingLoop(rebuilt,
                        (tp, record) -> retryAttempt.add(delivery(tp, record)));
                assertEquals(4, retry.runOnce(4, 8_192));
            }
            assertEquals(List.of("orders:0:0", "orders:0:1", "orders:1:0", "orders:1:1"), retryAttempt,
                    "the failed cycle redelivers the earlier partition's uncommitted records");
            try (RpcClient committedClient = broker.client();
                 SimpleConsumer committed = new SimpleConsumer(committedClient, "workers")) {
                committed.resume(List.of(zero, one));
                assertEquals(2, committed.position(zero));
                assertEquals(2, committed.position(one));
                assertEquals(OptionalLong.of(2), fetchOffset(committedClient, "workers", zero));
                assertEquals(OptionalLong.of(2), fetchOffset(committedClient, "workers", one));
                assertEquals(0, new ProcessingLoop(committed, (tp, record) -> {
                    throw new AssertionError("committed record was delivered again");
                }).runOnce(4, 8_192));
            }
        }
    }

    @Test
    void commitFailureAfterSideEffectsReplaysTheSameRecordsAtLeastOnce() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            TopicPartition tp = new TopicPartition("orders", 0);
            List<String> sideEffects = new ArrayList<>();
            try (OffsetBrokerFixture broker = new OffsetBrokerFixture(temp.root(), 1)) {
                broker.catalog().createTopic("orders", 1);
                try (RpcClient writer = broker.client()) {
                    TestSupport.append(writer, tp, TestSupport.values("record-", 2));
                }

                try (RpcClient client = broker.client();
                     SimpleConsumer consumer = new SimpleConsumer(client, "workers")) {
                    consumer.resume(List.of(tp));
                    ProcessingLoop loop = new ProcessingLoop(consumer, (partition, record) -> {
                        sideEffects.add(delivery(partition, record));
                        if (sideEffects.size() == 1) broker.close();
                    });
                    assertCode(ErrorCode.REQUEST_TIMEOUT, () -> loop.runOnce(2, 8_192));
                }
            }
            assertEquals(List.of("orders:0:0", "orders:0:1"), sideEffects,
                    "the failure happens after both polled records have caused side effects");

            try (OffsetBrokerFixture restarted = new OffsetBrokerFixture(temp.root(), 1)) {
                restarted.catalog().createTopic("orders", 1);
                try (RpcClient retryClient = restarted.client();
                     SimpleConsumer rebuilt = new SimpleConsumer(retryClient, "workers")) {
                    assertEquals(OptionalLong.empty(), fetchOffset(retryClient, "workers", tp),
                            "the failed COMMIT_OFFSET must leave no durable progress");
                    rebuilt.resume(List.of(tp));
                    assertEquals(0, rebuilt.position(tp));
                    ProcessingLoop retry = new ProcessingLoop(rebuilt,
                            (partition, record) -> sideEffects.add(delivery(partition, record)));
                    assertEquals(2, retry.runOnce(2, 8_192));
                    assertEquals(OptionalLong.of(2), fetchOffset(retryClient, "workers", tp));
                }

                assertEquals(List.of("orders:0:0", "orders:0:1", "orders:0:0", "orders:0:1"),
                        sideEffects,
                        "at-least-once processing may repeat completed external side effects after commit failure");
                try (RpcClient resumedClient = restarted.client();
                     SimpleConsumer resumed = new SimpleConsumer(resumedClient, "workers")) {
                    resumed.resume(List.of(tp));
                    assertEquals(2, resumed.position(tp));
                    assertEquals(0, new ProcessingLoop(resumed, (partition, record) -> {
                        throw new AssertionError("committed record was delivered again");
                    }).runOnce(2, 8_192));
                }
            }
        }
    }

    @Test
    void boundedRoundsProcessInTopicPartitionOffsetOrderAndCommitFinalPositions() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             OffsetBrokerFixture broker = new OffsetBrokerFixture(temp.root(), 1)) {
            broker.catalog().createTopic("orders", 2);
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            try (RpcClient writer = broker.client()) {
                TestSupport.append(writer, zero, TestSupport.values("p0-", 2));
                TestSupport.append(writer, one, TestSupport.values("p1-", 2));
            }

            List<String> deliveries = new ArrayList<>();
            try (RpcClient client = broker.client();
                 SimpleConsumer consumer = new SimpleConsumer(client, "workers")) {
                consumer.resume(List.of(one, zero));
                ProcessingLoop loop = new ProcessingLoop(consumer,
                        (tp, record) -> deliveries.add(delivery(tp, record)));
                assertEquals(1, loop.runOnce(1, 4_096));
                assertEquals(List.of("orders:0:0"), deliveries);
                assertEquals(3, loop.runOnce(10, 8_192));
                assertEquals(List.of("orders:0:0", "orders:0:1", "orders:1:0", "orders:1:1"), deliveries);
                assertEquals(2, consumer.position(zero));
                assertEquals(2, consumer.position(one));
                assertEquals(OptionalLong.of(2), fetchOffset(client, "workers", zero));
                assertEquals(OptionalLong.of(2), fetchOffset(client, "workers", one));
            }

            try (RpcClient resumedClient = broker.client();
                 SimpleConsumer resumed = new SimpleConsumer(resumedClient, "workers")) {
                resumed.resume(List.of(zero, one));
                assertEquals(2, resumed.position(zero));
                assertEquals(2, resumed.position(one));
                assertEquals(0, new ProcessingLoop(resumed, (tp, record) -> {
                    throw new AssertionError("an empty poll must not invoke the processor");
                }).runOnce(4, 8_192));
            }
        }
    }

    private static OptionalLong fetchOffset(RpcClient client, String group, TopicPartition tp) {
        Messages.Reply reply = client.call(new Messages.FetchOffsetRequest(new OffsetKey(group, tp)));
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.OffsetBody.class, reply.body()).nextOffset();
    }

    private static String delivery(TopicPartition tp, LogRecord record) {
        return tp.topic() + ":" + tp.partition() + ":" + record.offset();
    }
}
