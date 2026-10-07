package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.GroupConsumer;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import java.util.TreeSet;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step20Test {
    @Test
    void validatesOwnershipAndFencesAStaleGenerationWithoutChangingCommittedOffset() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 2);
             OffsetStore offsets = new OffsetStore(temp.root().resolve("direct-offsets"));
             GroupCoordinator groups = new GroupCoordinator(broker.catalog(), new RoundRobinAssignor(),
                     new ManualTimeSource(10), 1_000)) {
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            groups.join("workers", "a", "orders");
            groups.join("workers", "b", "orders");
            OffsetKey key = new OffsetKey("workers", zero);

            assertCode(ErrorCode.ILLEGAL_GENERATION,
                    () -> groups.commitOffset(new GroupToken("workers", "a", 1), zero, key, 100, offsets));
            assertEquals(OptionalLong.empty(), offsets.fetch(key));
            assertCode(ErrorCode.NOT_ASSIGNED,
                    () -> groups.commitOffset(new GroupToken("workers", "a", 2), one,
                            new OffsetKey("workers", one), 50, offsets));
            assertEquals(OptionalLong.empty(), offsets.fetch(new OffsetKey("workers", one)));

            groups.validate(new GroupToken("workers", "a", 2), zero);
            groups.commitOffset(new GroupToken("workers", "a", 2), zero, key, 7, offsets);
            groups.commitOffset(new GroupToken("workers", "b", 2), one,
                    new OffsetKey("workers", one), 8, offsets);
            assertEquals(OptionalLong.of(7), offsets.fetch(key));
            assertEquals(OptionalLong.of(8), offsets.fetch(new OffsetKey("workers", one)));
        }
    }

    @Test
    void loopbackGroupConsumersSplitPartitionsAndTakeOverFromCommittedPositionsAfterExpiry() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, clock, 100);
                 RpcClient writer = new RpcClient(broker.endpoint(), 3_000);
                 RpcClient firstClient = new RpcClient(broker.endpoint(), 3_000);
                 RpcClient secondClient = new RpcClient(broker.endpoint(), 3_000)) {
                broker.catalog().createTopic("orders", 4);
                List<TopicPartition> partitions = java.util.stream.IntStream.range(0, 4)
                        .mapToObj(index -> new TopicPartition("orders", index)).toList();
                for (TopicPartition tp : partitions) {
                    TestSupport.append(writer, tp, TestSupport.values("p" + tp.partition() + "-", 2));
                }

                try (GroupConsumer first = new GroupConsumer(firstClient, "workers", "a");
                     GroupConsumer second = new GroupConsumer(secondClient, "workers", "b")) {
                    first.subscribe("orders");
                    second.subscribe("orders");
                    Map<TopicPartition, List<io.simplekafka.model.LogRecord>> firstBatch = first.poll(8, 16_384);
                    Map<TopicPartition, List<io.simplekafka.model.LogRecord>> secondBatch = second.poll(8, 16_384);
                    assertEquals(List.of(partitions.get(0), partitions.get(2)), List.copyOf(firstBatch.keySet()));
                    assertEquals(List.of(partitions.get(1), partitions.get(3)), List.copyOf(secondBatch.keySet()));
                    assertEquals(8, firstBatch.values().stream().mapToInt(List::size).sum()
                            + secondBatch.values().stream().mapToInt(List::size).sum());
                    assertEquals(new TreeSet<>(partitions), new TreeSet<>(combinedKeys(firstBatch, secondBatch)));
                    first.commitSync();
                    second.commitSync();

                    clock.advanceMillis(99);
                    assertEquals(Map.of(), second.poll(8, 16_384));
                    clock.advanceMillis(1);
                    broker.expireGroups(); // The fixture scheduler may have processed this clock tick already.
                    for (TopicPartition tp : partitions) {
                        TestSupport.append(writer, tp,
                                List.of(TestSupport.value("after-expiry-" + tp.partition())));
                    }

                    Map<TopicPartition, List<io.simplekafka.model.LogRecord>> takenOver = second.poll(8, 16_384);
                    assertEquals(new TreeSet<>(partitions), new TreeSet<>(takenOver.keySet()));
                    for (TopicPartition tp : partitions) {
                        assertEquals(List.of(2L), takenOver.get(tp).stream()
                                .map(record -> record.offset()).toList());
                        assertEquals("after-expiry-" + tp.partition(),
                                new String(takenOver.get(tp).getFirst().data().value(), java.nio.charset.StandardCharsets.UTF_8));
                    }
                    second.commitSync();
                    assertCode(ErrorCode.UNKNOWN_MEMBER, first::commitSync);
                    first.subscribe("orders");
                }
            }
        }
    }

    private static List<TopicPartition> combinedKeys(Map<TopicPartition, ?> first,
                                                     Map<TopicPartition, ?> second) {
        List<TopicPartition> all = new ArrayList<>(first.keySet());
        all.addAll(second.keySet());
        return all;
    }
}
