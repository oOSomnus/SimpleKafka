package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.SimpleConsumer;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.OffsetBrokerFixture;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.nio.file.Path;
import java.util.List;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step15Test {
    @Test
    void offsetStorePersistsNextOffsetsPerGroupAndPartitionAndAllowsRewind() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path storePath = temp.root().resolve("offsets");
            TopicPartition orders0 = new TopicPartition("orders", 0);
            TopicPartition orders1 = new TopicPartition("orders", 1);
            OffsetKey worker0 = new OffsetKey("workers", orders0);
            OffsetKey otherGroup = new OffsetKey("other", orders0);
            OffsetKey worker1 = new OffsetKey("workers", orders1);

            try (OffsetStore store = new OffsetStore(storePath)) {
                assertEquals(OptionalLong.empty(), store.fetch(worker0));
                store.commit(worker0, 3);
                store.commit(otherGroup, 0);
                store.commit(worker1, 7);
                store.commit(worker0, 1);
                assertEquals(OptionalLong.of(1), store.fetch(worker0));
                assertCode(ErrorCode.INVALID_REQUEST, () -> store.commit(worker0, -1));
                assertEquals(OptionalLong.of(1), store.fetch(worker0));
            }

            try (OffsetStore reopened = new OffsetStore(storePath)) {
                assertEquals(OptionalLong.of(1), reopened.fetch(worker0));
                assertEquals(OptionalLong.of(0), reopened.fetch(otherGroup));
                assertEquals(OptionalLong.of(7), reopened.fetch(worker1));
            }
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
                }
            }

            try (OffsetBrokerFixture restarted = new OffsetBrokerFixture(temp.root(), 1)) {
                restarted.catalog().createTopic("orders", 1);
                try (RpcClient resumedClient = restarted.client();
                     SimpleConsumer resumed = new SimpleConsumer(resumedClient, "workers")) {
                    resumed.resume(List.of(tp));
                    assertEquals(2, resumed.position(tp));
                    assertEquals(List.of(2L), resumed.poll(2, 4_096).get(tp).stream()
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
}
