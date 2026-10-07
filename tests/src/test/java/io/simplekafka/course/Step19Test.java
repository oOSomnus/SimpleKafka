package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.GroupBrokerFixture;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.transport.RpcClient;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step19Test {
    @Test
    void rejectsStaleAndFutureHeartbeatsAndExpiresOnlyTheUnrefreshedMemberAtTheBoundary() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 3, clock, 100);
                 RpcClient client = broker.client()) {
                assertEquals(1, join(client, "workers", "a").generation());
                GroupAssignment joined = join(client, "workers", "b");
                assertEquals(2, joined.generation());

                clock.advanceMillis(99);
                assertEquals(ErrorCode.ILLEGAL_GENERATION,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "a", 1))).error());
                assertEquals(ErrorCode.ILLEGAL_GENERATION,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "a", 99))).error());
                assertEquals(ErrorCode.UNKNOWN_MEMBER,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "missing", 2))).error());
                assertEquals(ErrorCode.NONE,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "b", 2))).error());
                assertEquals(Set.of(), broker.expireGroups(), "no member expires at 1099");

                clock.advanceMillis(1);
                assertEquals(List.of("workers/a"), new ArrayList<>(broker.expireGroups()),
                        "a expires exactly at 1100; failed heartbeats did not refresh it");
                GroupAssignment afterExpiry = assignment(client, "workers", "b");
                assertEquals(3, afterExpiry.generation());
                assertEquals(Map.of("b", List.of(new TopicPartition("orders", 0),
                        new TopicPartition("orders", 1), new TopicPartition("orders", 2))),
                        afterExpiry.assignments());
                assertEquals(ErrorCode.UNKNOWN_MEMBER,
                        client.call(new Messages.HeartbeatRequest(new GroupToken("workers", "a", 3))).error());
            }
        }
    }

    @Test
    void idempotentJoinRefreshesHeartbeatButRejectedJoinDoesNot() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 2, clock, 100);
                 RpcClient client = broker.client()) {
                assertEquals(1, join(client, "workers", "a").generation());
                assertEquals(2, join(client, "workers", "b").generation());

                clock.advanceMillis(90);
                assertEquals(2, join(client, "workers", "a").generation(),
                        "idempotent join refreshes heartbeat without advancing generation");
                assertEquals(ErrorCode.INVALID_REQUEST,
                        client.call(new Messages.JoinGroupRequest("workers", "b", "other-topic")).error());

                clock.advanceMillis(10);
                assertEquals(List.of("workers/b"), new ArrayList<>(broker.expireGroups()),
                        "the rejected join does not extend b's original deadline");
                GroupAssignment afterBExpires = assignment(client, "workers", "a");
                assertEquals(3, afterBExpires.generation());
                assertEquals(Map.of("a", List.of(new TopicPartition("orders", 0),
                        new TopicPartition("orders", 1))), afterBExpires.assignments());

                clock.advanceMillis(89);
                assertEquals(Set.of(), broker.expireGroups(),
                        "a remains alive until 100 ms after its successful idempotent join");
                clock.advanceMillis(1);
                assertEquals(List.of("workers/a"), new ArrayList<>(broker.expireGroups()));
                assertEquals(5, join(client, "workers", "replacement").generation(),
                        "the empty group's generation survives expiration");
            }
        }
    }

    @Test
    void expirationOrdersAllIdentifiersAndAdvancesEachChangedGroupOnce() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker = new GroupBrokerFixture(temp.root(), 1, "orders", 3, clock, 100);
                 RpcClient client = broker.client()) {
                assertEquals(1, join(client, "zeta", "c").generation());
                assertEquals(2, join(client, "zeta", "a").generation());
                assertEquals(1, join(client, "alpha", "b").generation());
                assertEquals(2, join(client, "alpha", "c").generation());
                assertEquals(3, join(client, "alpha", "a").generation());

                clock.advanceMillis(100);
                assertEquals(List.of("alpha/a", "alpha/b", "alpha/c", "zeta/a", "zeta/c"),
                        new ArrayList<>(broker.expireGroups()));
                assertEquals(Set.of(), broker.expireGroups(), "a repeated expiry is a no-op");
                assertEquals(5, join(client, "alpha", "new-member").generation(),
                        "three expired members change alpha's generation only once");
                assertEquals(4, join(client, "zeta", "new-member").generation(),
                        "two expired members change zeta's generation only once");
            }
        }
    }

    private static GroupAssignment join(RpcClient client, String group, String member) {
        Messages.Reply reply = client.call(new Messages.JoinGroupRequest(group, member, "orders"));
        assertEquals(ErrorCode.NONE, reply.error());
        return ((Messages.GroupBody) reply.body()).assignment();
    }

    private static GroupAssignment assignment(RpcClient client, String group, String member) {
        Messages.Reply reply = client.call(new Messages.GroupAssignmentRequest(group, member));
        assertEquals(ErrorCode.NONE, reply.error());
        return ((Messages.GroupBody) reply.body()).assignment();
    }
}
