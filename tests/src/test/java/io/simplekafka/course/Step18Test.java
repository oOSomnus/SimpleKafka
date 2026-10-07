package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.GroupBrokerFixture;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;

class Step18Test {
    @Test
    void tcpMembershipChangesReassignAllPartitionsAndAdvanceGenerationExactlyOnce() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 3,
                     new ManualTimeSource(100), 500);
             RpcClient client = broker.client()) {
            GroupAssignment first = join(client, "a");
            assertEquals(1, first.generation());
            assertEquals(3, first.assignments().get("a").size());
            assertEquals(1, join(client, "a").generation(),
                    "an idempotent join only refreshes the member heartbeat");

            GroupAssignment second = join(client, "b");
            assertEquals(2, second.generation());
            assertEquals(second, assignment(client, "a"));
            assertEquals(List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 2)),
                    second.assignments().get("a"));
            assertEquals(List.of(new TopicPartition("orders", 1)), second.assignments().get("b"));

            GroupAssignment afterLeave = assignmentReply(client.call(new Messages.LeaveGroupRequest("workers", "a")));
            assertEquals(3, afterLeave.generation());
            assertEquals(List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1),
                    new TopicPartition("orders", 2)), afterLeave.assignments().get("b"));
            GroupAssignment empty = assignmentReply(client.call(new Messages.LeaveGroupRequest("workers", "b")));
            assertEquals(4, empty.generation());
            assertEquals(java.util.Map.of(), empty.assignments());
            assertEquals(5, join(client, "c").generation(),
                    "an empty group's generation is retained for the next member");

            assertEquals(ErrorCode.INVALID_REQUEST,
                    client.call(new Messages.JoinGroupRequest("workers", "d", "other-topic")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.LeaveGroupRequest("workers", "unknown")).error());
            assertEquals(ErrorCode.UNKNOWN_MEMBER,
                    client.call(new Messages.GroupAssignmentRequest("workers", "unknown")).error());
        }
    }

    private static GroupAssignment join(RpcClient client, String member) {
        return assignmentReply(client.call(new Messages.JoinGroupRequest("workers", member, "orders")));
    }

    private static GroupAssignment assignment(RpcClient client, String member) {
        return assignmentReply(client.call(new Messages.GroupAssignmentRequest("workers", member)));
    }

    private static GroupAssignment assignmentReply(Messages.Reply reply) {
        assertEquals(ErrorCode.NONE, reply.error());
        return assertInstanceOf(Messages.GroupBody.class, reply.body()).assignment();
    }
}
