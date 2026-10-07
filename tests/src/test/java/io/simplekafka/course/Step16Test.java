package io.simplekafka.course;

import io.simplekafka.client.ProcessingLoop;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.OffsetBrokerFixture;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Step16Test {
    @Test
    void failedProcessingDoesNotCommitAndARebuiltConsumerRedeliversTheUncommittedBatch() throws Exception {
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
            IllegalStateException processingFailure = new IllegalStateException("processor failed at partition 0 offset 1");
            try (RpcClient firstClient = broker.client();
                 SimpleConsumer consumer = new SimpleConsumer(firstClient, "workers")) {
                consumer.assign(List.of(one, zero));
                ProcessingLoop loop = new ProcessingLoop(consumer, (tp, record) -> {
                    firstAttempt.add(delivery(tp, record));
                    if (tp.equals(zero) && record.offset() == 1) throw processingFailure;
                });
                assertSame(processingFailure, assertThrows(IllegalStateException.class,
                        () -> loop.runOnce(4, 8_192)));
            }

            assertEquals(List.of("0:0", "0:1"), firstAttempt);

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
            assertEquals(List.of("0:0", "0:1", "1:0", "1:1"), retryAttempt,
                    "partitions and their records are processed deterministically");

            try (RpcClient committedClient = broker.client();
                 SimpleConsumer committed = new SimpleConsumer(committedClient, "workers")) {
                committed.resume(List.of(zero, one));
                assertEquals(2, committed.position(zero));
                assertEquals(2, committed.position(one));
                assertEquals(0, new ProcessingLoop(committed, (tp, record) -> {
                    throw new AssertionError("committed record was delivered again");
                }).runOnce(4, 8_192));
            }
        }
    }

    private static String delivery(TopicPartition tp, LogRecord record) {
        return tp.partition() + ":" + record.offset();
    }
}
