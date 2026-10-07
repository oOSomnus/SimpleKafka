package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step14Test {
    @Test
    void assignsDeduplicatedPartitionsPollsWithinOneRoundBudgetAndSupportsSeek() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 2);
             RpcClient client = new RpcClient(broker.endpoint(), 3_000);
             SimpleConsumer consumer = new SimpleConsumer(client, "manual-group")) {
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            TestSupport.append(client, zero, List.of(new RecordData(null, new byte[0], 0),
                    new RecordData(null, new byte[40], 0)));
            TestSupport.append(client, one, Collections.nCopies(3, new RecordData(null, new byte[0], 0)));

            consumer.assign(List.of(one, zero, one));
            Map<TopicPartition, List<LogRecord>> first = consumer.poll(3, 64);
            assertEquals(List.of(zero, one), List.copyOf(first.keySet()), "partitions are fetched in sorted order");
            assertEquals(List.of(0L), first.get(zero).stream().map(LogRecord::offset).toList());
            assertEquals(List.of(0L), first.get(one).stream().map(LogRecord::offset).toList());
            assertEquals(1, consumer.position(zero));
            assertEquals(1, consumer.position(one));

            Map<TopicPartition, List<LogRecord>> second = consumer.poll(2, 128);
            assertEquals(List.of(zero, one), List.copyOf(second.keySet()));
            assertEquals(List.of(1L), second.get(zero).stream().map(LogRecord::offset).toList());
            assertEquals(List.of(1L), second.get(one).stream().map(LogRecord::offset).toList());
            assertEquals(2, consumer.position(zero));
            assertEquals(2, consumer.position(one));

            Map<TopicPartition, List<LogRecord>> third = consumer.poll(3, 4_096);
            assertEquals(List.of(one), List.copyOf(third.keySet()));
            assertEquals(List.of(2L), third.get(one).stream().map(LogRecord::offset).toList());
            assertEquals(2, consumer.position(zero), "an empty fetch does not advance another partition");
            assertEquals(3, consumer.position(one));
            assertTrue(consumer.poll(3, 4_096).isEmpty());
            assertEquals(3, consumer.position(one));

            consumer.seek(one, 0);
            Map<TopicPartition, List<LogRecord>> reread = consumer.poll(1, 4_096);
            assertEquals(List.of(0L), reread.get(one).stream().map(LogRecord::offset).toList());
            assertEquals(1, consumer.position(one));
            assertCode(ErrorCode.NOT_ASSIGNED, () -> consumer.seek(new TopicPartition("orders", 8), 0));
            assertCode(ErrorCode.INVALID_REQUEST, () -> consumer.poll(0, 4_096));
            consumer.seek(one, 99);
            assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> consumer.poll(1, 4_096));
            assertEquals(99, consumer.position(one), "a failed fetch does not change the sought position");
        }
    }
}
