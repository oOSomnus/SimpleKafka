package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.GroupConsumer;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.GroupBrokerFixture;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.support.TestSupport;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import java.util.Map;
import java.util.OptionalLong;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class Step20Test {
    @Test
    void coordinatorFencesCommitsByGenerationOwnershipAndMatchingOffsetKey() throws Exception {
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
            assertCode(ErrorCode.INVALID_REQUEST,
                    () -> groups.commitOffset(new GroupToken("workers", "a", 2), zero,
                            new OffsetKey("other-workers", zero), 50, offsets));
            assertEquals(OptionalLong.empty(), offsets.fetch(new OffsetKey("other-workers", zero)));

            groups.validate(new GroupToken("workers", "a", 2), zero);
            groups.commitOffset(new GroupToken("workers", "a", 2), zero, key, 7, offsets);
            groups.commitOffset(new GroupToken("workers", "b", 2), one,
                    new OffsetKey("workers", one), 8, offsets);
            assertEquals(OptionalLong.of(7), offsets.fetch(key));
            assertEquals(OptionalLong.of(8), offsets.fetch(new OffsetKey("workers", one)));
        }
    }

    @Test
    void rawTcpFetchAndCommitTokensFenceInvalidOwnersWithoutChangingOffsets() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 2, clock, 100);
                 RpcClient writer = broker.client();
                 RpcClient client = broker.client()) {
                TopicPartition zero = new TopicPartition("orders", 0);
                TopicPartition one = new TopicPartition("orders", 1);
                TestSupport.append(writer, zero, TestSupport.values("p0-", 2));
                TestSupport.append(writer, one, List.of(TestSupport.value("p1-0")));

                GroupAssignment first = join(client, "workers", "a");
                assertEquals(1, first.generation());
                GroupAssignment second = join(client, "workers", "b");
                assertEquals(2, second.generation());
                GroupAssignment third = join(client, "workers", "c");
                assertEquals(3, third.generation());
                Map<String, List<TopicPartition>> owners = Map.of(
                        "a", List.of(zero), "b", List.of(one), "c", List.of());
                assertEquals(owners, third.assignments());
                for (String member : List.of("a", "b", "c")) {
                    assertEquals(third, assignment(client, "workers", member));
                }

                OffsetKey zeroKey = new OffsetKey("workers", zero);
                OffsetKey oneKey = new OffsetKey("workers", one);
                GroupToken owner = new GroupToken("workers", "a", 3);
                Messages.Reply validFetch = client.call(
                        new Messages.FetchRequest(zero, 0, 0, 10, 4_096, owner));
                assertEquals(ErrorCode.NONE, validFetch.error());
                assertEquals(List.of(0L, 1L), fetchBody(validFetch).records().stream()
                        .map(record -> record.offset()).toList());
                assertEquals(OptionalLong.empty(), fetchOffset(client, zeroKey));
                assertEquals(OptionalLong.empty(), fetchOffset(client, oneKey));

                assertFetchFailure(client, new Messages.FetchRequest(zero, 0, 0, 10, 4_096,
                        new GroupToken("workers", "a", 2)), ErrorCode.ILLEGAL_GENERATION, zeroKey, oneKey);
                assertFetchFailure(client, new Messages.FetchRequest(zero, 0, 0, 10, 4_096,
                        new GroupToken("workers", "a", 99)), ErrorCode.ILLEGAL_GENERATION, zeroKey, oneKey);
                assertFetchFailure(client, new Messages.FetchRequest(zero, 0, 0, 10, 4_096,
                        new GroupToken("workers", "missing", 3)), ErrorCode.UNKNOWN_MEMBER, zeroKey, oneKey);
                assertFetchFailure(client, new Messages.FetchRequest(zero, 0, 0, 10, 4_096,
                        new GroupToken("workers", "b", 3)), ErrorCode.NOT_ASSIGNED, zeroKey, oneKey);
                assertFetchFailure(client, new Messages.FetchRequest(zero, 0, 0, 10, 4_096,
                        new GroupToken("workers", "c", 3)), ErrorCode.NOT_ASSIGNED, zeroKey, oneKey);
                Messages.Reply validSecondOwnerFetch = client.call(
                        new Messages.FetchRequest(one, 0, 0, 10, 4_096,
                                new GroupToken("workers", "b", 3)));
                assertEquals(ErrorCode.NONE, validSecondOwnerFetch.error());
                assertEquals(List.of(0L), fetchBody(validSecondOwnerFetch).records().stream()
                        .map(record -> record.offset()).toList());
                assertEquals(ErrorCode.NONE,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "c", 3))).error());
                assertEquals(third, assignment(client, "workers", "c"),
                        "a successful empty-owner heartbeat does not change assignment");

                assertEquals(ErrorCode.NONE,
                        client.call(new Messages.CommitOffsetRequest(zeroKey, 2, owner)).error());
                assertCommitFailure(client, new Messages.CommitOffsetRequest(zeroKey, 5,
                        new GroupToken("workers", "a", 2)), ErrorCode.ILLEGAL_GENERATION, zeroKey, oneKey);
                assertCommitFailure(client, new Messages.CommitOffsetRequest(zeroKey, 5,
                        new GroupToken("workers", "a", 99)), ErrorCode.ILLEGAL_GENERATION, zeroKey, oneKey);
                assertCommitFailure(client, new Messages.CommitOffsetRequest(zeroKey, 5,
                        new GroupToken("workers", "b", 3)), ErrorCode.NOT_ASSIGNED, zeroKey, oneKey);
                assertCommitFailure(client, new Messages.CommitOffsetRequest(zeroKey, 5,
                        new GroupToken("workers", "missing", 3)), ErrorCode.UNKNOWN_MEMBER, zeroKey, oneKey);

                OffsetKey otherGroupKey = new OffsetKey("other-workers", zero);
                Messages.Reply mismatchedGroup = client.call(
                        new Messages.CommitOffsetRequest(otherGroupKey, 5, owner));
                assertEquals(ErrorCode.INVALID_REQUEST, mismatchedGroup.error());
                assertInstanceOf(Messages.ErrorBody.class, mismatchedGroup.body());
                assertEquals(OptionalLong.of(2), fetchOffset(client, zeroKey));
                assertEquals(OptionalLong.empty(), fetchOffset(client, oneKey));
                assertEquals(OptionalLong.empty(), fetchOffset(client, otherGroupKey),
                        "a mismatched group token writes neither offset key");

                try (RpcClient emptyClient = broker.client();
                     GroupConsumer empty = new GroupConsumer(emptyClient, "workers", "c")) {
                    empty.subscribe("orders");
                    assertEquals(Map.of(), empty.poll(10, 4_096));
                    empty.commitSync();
                }

                Messages.Reply manualFetch = client.call(
                        new Messages.FetchRequest(one, 0, 0, 10, 4_096, null));
                assertEquals(ErrorCode.NONE, manualFetch.error(), "null-token manual FETCH remains supported");
                assertEquals(List.of("p1-0"), TestSupport.values(fetchBody(manualFetch).records()));
                OffsetKey manualKey = new OffsetKey("manual-workers", one);
                assertEquals(ErrorCode.NONE,
                        client.call(new Messages.CommitOffsetRequest(manualKey, 1, null)).error(),
                        "null-token manual COMMIT remains supported");
                assertEquals(OptionalLong.of(1), fetchOffset(client, manualKey));
            }
        }
    }

    @Test
    void groupConsumerPreservesRetainedPositionDropsRevokedPartitionsAndRecoversAfterExpiryAndRestart()
            throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            TopicPartition zero = new TopicPartition("orders", 0);
            TopicPartition one = new TopicPartition("orders", 1);
            OffsetKey zeroKey = new OffsetKey("workers", zero);
            OffsetKey oneKey = new OffsetKey("workers", one);

            try (GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 2, clock, 100);
                 RpcClient writer = broker.client();
                 RpcClient firstClient = broker.client();
                 RpcClient secondClient = broker.client();
                 RpcClient inspector = broker.client();
                 GroupConsumer first = new GroupConsumer(firstClient, "workers", "a");
                 GroupConsumer second = new GroupConsumer(secondClient, "workers", "b")) {
                TestSupport.append(writer, zero, List.of(TestSupport.value("p0-0")));
                TestSupport.append(writer, one, TestSupport.values("p1-", 9));
                assertEquals(ErrorCode.NONE,
                        writer.call(new Messages.CommitOffsetRequest(oneKey, 7, null)).error());

                first.subscribe("orders");
                Map<TopicPartition, List<io.simplekafka.model.LogRecord>> firstRecord = first.poll(1, 4_096);
                assertEquals(List.of(0L), firstRecord.get(zero).stream()
                        .map(record -> record.offset()).toList());
                Map<TopicPartition, List<io.simplekafka.model.LogRecord>> p1Record = first.poll(1, 4_096);
                assertEquals(List.of(7L), p1Record.get(one).stream()
                        .map(record -> record.offset()).toList());
                assertEquals(OptionalLong.empty(), fetchOffset(inspector, zeroKey),
                        "a's p0 position one is still uncommitted");
                assertEquals(OptionalLong.of(7), fetchOffset(inspector, oneKey));

                second.subscribe("orders");
                TestSupport.append(writer, zero, List.of(TestSupport.value("p0-1")));
                Map<TopicPartition, List<io.simplekafka.model.LogRecord>> afterRebalance = first.poll(1, 4_096);
                assertEquals(List.of(zero), List.copyOf(afterRebalance.keySet()),
                        "the revoked p1 is removed from a's replacement assignment");
                assertEquals(List.of(1L), afterRebalance.get(zero).stream()
                        .map(record -> record.offset()).toList(),
                        "a's uncommitted p0 position survives and the next poll begins at offset one");
                first.commitSync();
                assertEquals(OptionalLong.of(2), fetchOffset(inspector, zeroKey));
                assertEquals(OptionalLong.of(7), fetchOffset(inspector, oneKey),
                        "a does not commit its revoked p1 position over the stored offset");

                Map<TopicPartition, List<io.simplekafka.model.LogRecord>> bTakeover = second.poll(1, 4_096);
                assertEquals(List.of(one), List.copyOf(bTakeover.keySet()));
                assertEquals(List.of(7L), bTakeover.get(one).stream().map(record -> record.offset()).toList(),
                        "b's new p1 assignment starts from its committed next offset");

                clock.advanceMillis(99);
                Map<TopicPartition, List<io.simplekafka.model.LogRecord>> refreshedB = second.poll(1, 4_096);
                second.commitSync();
                assertEquals(OptionalLong.of(9), fetchOffset(inspector, oneKey),
                        "b persists its final p1 position after reading offset eight");
                assertEquals(List.of(8L), refreshedB.get(one).stream().map(record -> record.offset()).toList());
                clock.advanceMillis(1);
                assertEquals(List.of("workers/a"), new java.util.ArrayList<>(broker.expireGroups()));
                assertCode(ErrorCode.UNKNOWN_MEMBER, () -> first.poll(1, 4_096));
                assertCode(ErrorCode.UNKNOWN_MEMBER, first::commitSync);
                assertEquals(ErrorCode.UNKNOWN_MEMBER,
                        inspector.call(new Messages.GroupAssignmentRequest("workers", "a")).error(),
                        "failed poll and commit do not silently rejoin a");

                first.subscribe("orders");
                assertEquals(ErrorCode.NONE,
                        inspector.call(new Messages.GroupAssignmentRequest("workers", "a")).error(),
                        "only explicit subscribe rejoins the expired member");
                assertEquals(OptionalLong.of(2), fetchOffset(inspector, zeroKey));
                assertEquals(OptionalLong.of(9), fetchOffset(inspector, oneKey));
            }

            try (GroupBrokerFixture restarted = new GroupBrokerFixture(temp.root(), 1, "orders", 2, clock, 100);
                 RpcClient writer = restarted.client();
                 RpcClient readerClient = restarted.client();
                 RpcClient inspectRestart = restarted.client();
                 GroupConsumer reader = new GroupConsumer(readerClient, "workers", "a")) {
                assertEquals(ErrorCode.UNKNOWN_MEMBER,
                        inspectRestart.call(new Messages.GroupAssignmentRequest("workers", "a")).error(),
                        "membership is process-local and must be rejoined after fixture restart");
                reader.subscribe("orders");
                TestSupport.append(writer, zero, List.of(TestSupport.value("p0-after-restart")));
                TestSupport.append(writer, one, List.of(TestSupport.value("p1-after-restart")));
                GroupAssignment rejoined = assignment(inspectRestart, "workers", "a");
                assertEquals(1, rejoined.generation());
                assertEquals(Map.of("a", List.of(zero, one)), rejoined.assignments());
                Map<TopicPartition, List<io.simplekafka.model.LogRecord>> resumed = reader.poll(2, 4_096);
                assertEquals(List.of(2L), resumed.get(zero).stream().map(record -> record.offset()).toList());
                assertEquals(List.of(9L), resumed.get(one).stream().map(record -> record.offset()).toList());
                assertEquals(OptionalLong.of(2), fetchOffset(inspectRestart, zeroKey));
                assertEquals(OptionalLong.of(9), fetchOffset(inspectRestart, oneKey));
            }
        }
    }

    private static GroupAssignment join(RpcClient client, String group, String member) {
        Messages.Reply reply = client.call(new Messages.JoinGroupRequest(group, member, "orders"));
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.GroupBody.class, reply.body()).assignment();
    }

    private static GroupAssignment assignment(RpcClient client, String group, String member) {
        Messages.Reply reply = client.call(new Messages.GroupAssignmentRequest(group, member));
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.GroupBody.class, reply.body()).assignment();
    }

    private static Messages.FetchBody fetchBody(Messages.Reply reply) {
        return assertInstanceOf(Messages.FetchBody.class, reply.body());
    }

    private static void assertFetchFailure(RpcClient client, Messages.FetchRequest request,
                                           ErrorCode expected, OffsetKey zeroKey, OffsetKey oneKey) {
        Messages.Reply reply = client.call(request);
        assertEquals(expected, reply.error());
        assertInstanceOf(Messages.ErrorBody.class, reply.body(), "failed FETCH returns no records");
        assertEquals(OptionalLong.empty(), fetchOffset(client, zeroKey));
        assertEquals(OptionalLong.empty(), fetchOffset(client, oneKey));
    }

    private static void assertCommitFailure(RpcClient client, Messages.CommitOffsetRequest request,
                                            ErrorCode expected, OffsetKey zeroKey, OffsetKey oneKey) {
        Messages.Reply reply = client.call(request);
        assertEquals(expected, reply.error());
        assertInstanceOf(Messages.ErrorBody.class, reply.body());
        assertEquals(OptionalLong.of(2), fetchOffset(client, zeroKey));
        assertEquals(OptionalLong.empty(), fetchOffset(client, oneKey));
    }

    private static OptionalLong fetchOffset(RpcClient client, OffsetKey key) {
        Messages.Reply reply = client.call(new Messages.FetchOffsetRequest(key));
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.OffsetBody.class, reply.body()).nextOffset();
    }
}
