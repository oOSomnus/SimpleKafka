package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.GroupBrokerFixture;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step19Test {
    @Test
    void expiresAtTheExactBoundaryAndDoesNotExpireAHeartbeatRefreshedMember() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 1, 0, "orders", 3)) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupCoordinator groups = new GroupCoordinator(broker.catalog(), new RoundRobinAssignor(), clock, 100)) {
                groups.join("workers", "a", "orders");
                groups.join("workers", "b", "orders");
                GroupAssignment beforeExpiry = groups.join("workers", "c", "orders");
                assertEquals(3, beforeExpiry.generation());

                assertCode(ErrorCode.ILLEGAL_GENERATION,
                        () -> groups.heartbeat(new GroupToken("workers", "a", 1)));
                assertCode(ErrorCode.UNKNOWN_MEMBER,
                        () -> groups.heartbeat(new GroupToken("workers", "missing", 3)));

                clock.advanceMillis(99);
                groups.heartbeat(new GroupToken("workers", "b", 3));
                assertEquals(Set.of(), groups.expire(), "members are retained before the timeout boundary");
                clock.advanceMillis(1);

                assertEquals(Set.of("workers/a", "workers/c"), groups.expire());
                GroupAssignment afterExpiry = groups.assignment("workers", "b");
                assertEquals(4, afterExpiry.generation(), "multiple expirations cause one generation change");
                assertEquals(List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 1),
                        new TopicPartition("orders", 2)), afterExpiry.assignments().get("b"));
                assertCode(ErrorCode.UNKNOWN_MEMBER,
                        () -> groups.heartbeat(new GroupToken("workers", "a", 4)));
            }
        }
    }

    @Test
    void heartbeatApiUsesTheSameGenerationFenceAndInjectedExpiryClockOverTcp() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 3, clock, 100);
                 RpcClient client = broker.client()) {
                join(client, "a");
                join(client, "b");
                GroupAssignment joined = join(client, "c");
                assertEquals(3, joined.generation());

                assertEquals(ErrorCode.ILLEGAL_GENERATION,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "a", 1))).error());
                assertEquals(ErrorCode.UNKNOWN_MEMBER,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "missing", 3))).error());

                clock.advanceMillis(99);
                assertEquals(ErrorCode.NONE,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "b", 3))).error());
                assertEquals(Set.of(), broker.expireGroups());
                clock.advanceMillis(1);
                assertEquals(Set.of("workers/a", "workers/c"), broker.expireGroups());

                Messages.Reply current = client.call(new Messages.GroupAssignmentRequest("workers", "b"));
                assertEquals(ErrorCode.NONE, current.error());
                GroupAssignment assignment = ((Messages.GroupBody) current.body()).assignment();
                assertEquals(4, assignment.generation());
                assertEquals(3, assignment.assignments().get("b").size());
                assertEquals(ErrorCode.UNKNOWN_MEMBER,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "a", 4))).error());
            }
        }
    }

    private static GroupAssignment join(RpcClient client, String member) {
        Messages.Reply reply = client.call(new Messages.JoinGroupRequest("workers", member, "orders"));
        assertEquals(ErrorCode.NONE, reply.error());
        return ((Messages.GroupBody) reply.body()).assignment();
    }
}
