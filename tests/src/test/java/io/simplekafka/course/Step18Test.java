package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.GroupBrokerFixture;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import java.util.OptionalLong;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class Step18Test {
    @Test
    void membershipChangesRebalanceEverySurvivorAndRetainEmptyGroupGenerationOverTcp() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 3,
                     new ManualTimeSource(100), 500);
             RpcClient client = broker.client()) {
            List<TopicPartition> all = List.of(
                    new TopicPartition("orders", 0),
                    new TopicPartition("orders", 1),
                    new TopicPartition("orders", 2));

            GroupAssignment first = join(client, "workers", "a", "orders");
            assertEquals(1, first.generation());
            assertEquals(Map.of("a", all), first.assignments());
            assertEquals(first, join(client, "workers", "a", "orders"),
                    "an idempotent join refreshes the member without changing topology or generation");

            GroupAssignment second = join(client, "workers", "b", "orders");
            Map<String, List<TopicPartition>> twoMembers = Map.of(
                    "a", List.of(all.get(0), all.get(2)),
                    "b", List.of(all.get(1)));
            assertEquals(2, second.generation());
            assertEquals(twoMembers, second.assignments());
            assertEquals(second, assignment(client, "workers", "a"));
            assertEquals(second, assignment(client, "workers", "b"));

            assertEquals(ErrorCode.INVALID_REQUEST,
                    client.call(new Messages.JoinGroupRequest("workers", "cross-topic", "another-topic")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.LeaveGroupRequest("workers", "unknown")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.LeaveGroupRequest("missing-group", "unknown")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.GroupAssignmentRequest("workers", "unknown")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.GroupAssignmentRequest("missing-group", "unknown")).error());
            assertEquals(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                    client.call(new Messages.JoinGroupRequest("unknown-topic-group", "ghost", "missing-topic")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.GroupAssignmentRequest("unknown-topic-group", "ghost")).error());
            GroupAssignment afterUnknownTopicJoin = join(client, "unknown-topic-group", "ghost", "orders");
            assertEquals(1, afterUnknownTopicJoin.generation(),
                    "a rejected first join does not register a group or consume its generation");
            assertEquals(Map.of("ghost", all), afterUnknownTopicJoin.assignments());
            assertEquals(second, assignment(client, "workers", "a"),
                    "rejected joins and queries do not alter surviving assignments");
            assertEquals(second, assignment(client, "workers", "b"));

            GroupAssignment third = join(client, "workers", "c", "orders");
            assertEquals(3, third.generation(), "failed joins must not consume a generation");
            Map<String, List<TopicPartition>> threeMembers = Map.of(
                    "a", List.of(all.get(0)),
                    "b", List.of(all.get(1)),
                    "c", List.of(all.get(2)));
            assertEquals(threeMembers, third.assignments());
            for (String member : List.of("a", "b", "c")) {
                assertEquals(third, assignment(client, "workers", member));
            }

            GroupAssignment afterLeaveC = leave(client, "workers", "c");
            assertEquals(4, afterLeaveC.generation());
            Map<String, List<TopicPartition>> afterC = Map.of(
                    "a", List.of(all.get(0), all.get(2)),
                    "b", List.of(all.get(1)));
            assertEquals(afterC, afterLeaveC.assignments());
            assertEquals(afterLeaveC, assignment(client, "workers", "a"));
            assertEquals(afterLeaveC, assignment(client, "workers", "b"));

            GroupAssignment afterLeaveA = leave(client, "workers", "a");
            assertEquals(5, afterLeaveA.generation());
            assertEquals(Map.of("b", all), afterLeaveA.assignments());
            assertEquals(afterLeaveA, assignment(client, "workers", "b"));
            OffsetKey retainedOffset = new OffsetKey("workers", all.getFirst());
            Messages.Reply commit = client.call(new Messages.CommitOffsetRequest(retainedOffset, 3, null));
            assertEquals(ErrorCode.NONE, commit.error());


            GroupAssignment empty = leave(client, "workers", "b");
            assertEquals(6, empty.generation());
            assertEquals(Map.of(), empty.assignments());
            Messages.Reply emptyOffset = client.call(new Messages.FetchOffsetRequest(retainedOffset));
            assertEquals(ErrorCode.NONE, emptyOffset.error());
            assertEquals(OptionalLong.of(3),
                    assertInstanceOf(Messages.OffsetBody.class, emptyOffset.body()).nextOffset());
            assertEquals(ErrorCode.INVALID_REQUEST,
                    client.call(new Messages.JoinGroupRequest("workers", "cross-topic", "another-topic")).error());
            GroupAssignment afterEmptyRejoin = join(client, "workers", "c", "orders");
            assertEquals(7, afterEmptyRejoin.generation(),
                    "an empty group retains its generation for the next member");
            assertEquals(Map.of("c", all), afterEmptyRejoin.assignments());
            Messages.Reply rejoinedOffset = client.call(new Messages.FetchOffsetRequest(retainedOffset));
            assertEquals(ErrorCode.NONE, rejoinedOffset.error());
            assertEquals(OptionalLong.of(3),
                    assertInstanceOf(Messages.OffsetBody.class, rejoinedOffset.body()).nextOffset());
        }
    }

    private static GroupAssignment join(RpcClient client, String group, String member, String topic) {
        return assignmentReply(client.call(new Messages.JoinGroupRequest(group, member, topic)));
    }

    private static GroupAssignment leave(RpcClient client, String group, String member) {
        return assignmentReply(client.call(new Messages.LeaveGroupRequest(group, member)));
    }

    private static GroupAssignment assignment(RpcClient client, String group, String member) {
        return assignmentReply(client.call(new Messages.GroupAssignmentRequest(group, member)));
    }

    private static GroupAssignment assignmentReply(Messages.Reply reply) {
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.GroupBody.class, reply.body()).assignment();
    }
}
