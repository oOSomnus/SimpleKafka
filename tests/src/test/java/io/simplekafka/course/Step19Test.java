package io.simplekafka.course;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.support.GroupBrokerFixture;
import io.simplekafka.support.ManualTimeSource;
import io.simplekafka.support.TempDirectory;
import io.simplekafka.transport.RpcClient;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

class Step19Test {
    @Test
    @DisplayName("Replicated cluster expires idle members without incoming requests")
    void replicatedClusterExpiresIdleMembersWithoutIncomingRequests() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(0);
            try (ClusterHarness cluster = new ClusterHarness(temp.root(), clock)) {
                cluster.start();
                cluster.createTopic("orders", 1, 2);
                try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
                    GroupAssignment joined = join(client, "workers", "a");
                    assertEquals(1, joined.generation());
                    assertEquals(
                            Map.of("a", List.of(new TopicPartition("orders", 0))),
                            joined.assignments());

                    clock.setMillis(20_000);
                    long deadline = System.nanoTime() + TimeUnit.SECONDS.toNanos(5);
                    ErrorCode assignmentError;
                    do {
                        Messages.Reply reply =
                                client.call(new Messages.GroupAssignmentRequest("workers", "a"));
                        assignmentError = reply.error();
                        if (assignmentError == ErrorCode.UNKNOWN_MEMBER) break;
                        assertEquals(ErrorCode.NONE, assignmentError);
                        Thread.sleep(10);
                    } while (System.nanoTime() < deadline);
                    assertEquals(
                            ErrorCode.UNKNOWN_MEMBER,
                            assignmentError,
                            "the background worker must expire idle members without a hook or heartbeat");

                    GroupAssignment replacement = join(client, "workers", "b");
                    assertEquals(3, replacement.generation());
                    assertEquals(
                            Map.of("b", List.of(new TopicPartition("orders", 0))),
                            replacement.assignments());
                    assertEquals(
                            ErrorCode.UNKNOWN_MEMBER,
                            client.call(
                                            new Messages.HeartbeatRequest(
                                                    new GroupToken("workers", "a", 3)))
                                    .error());
                }
            }
        }
    }

    @Test
    @DisplayName("Expire groups provides deterministic cluster expiry and checks lifecycle")
    void expireGroupsProvidesDeterministicClusterExpiryAndChecksLifecycle() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(0);
            ClusterHarness cluster = new ClusterHarness(temp.root(), clock);
            try {
                cluster.start();
                cluster.createTopic("orders", 1, 2);
                try (RpcClient client = new RpcClient(cluster.endpoint(1), 3_000)) {
                    GroupAssignment joined = join(client, "workers", "a");
                    assertEquals(1, joined.generation());
                    assertEquals(Set.of(), cluster.expireGroups());
                    assertEquals(
                            Map.of("a", List.of(new TopicPartition("orders", 0))),
                            assignment(client, "workers", "a").assignments());

                    clock.setMillis(9_999);
                    assertEquals(Set.of(), cluster.expireGroups());
                    assertEquals(
                            Map.of("a", List.of(new TopicPartition("orders", 0))),
                            assignment(client, "workers", "a").assignments());

                    clock.setMillis(10_000);
                    cluster.expireGroups();
                    assertEquals(
                            ErrorCode.UNKNOWN_MEMBER,
                            client.call(
                                            new Messages.HeartbeatRequest(
                                                    new GroupToken("workers", "a", 1)))
                                    .error());
                    GroupAssignment replacement = join(client, "workers", "b");
                    assertEquals(3, replacement.generation());
                    assertEquals(
                            Map.of("b", List.of(new TopicPartition("orders", 0))),
                            replacement.assignments());
                    assertEquals(Set.of(), cluster.expireGroups());
                }
            } finally {
                cluster.close();
            }
            assertThrows(IllegalStateException.class, cluster::expireGroups);
            cluster.close();
        }
    }

    @Test
    @DisplayName(
            "Rejects stale and future heartbeats and expires only the unrefreshed member at the boundary")
    void rejectsStaleAndFutureHeartbeatsAndExpiresOnlyTheUnrefreshedMemberAtTheBoundary()
            throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker =
                            new GroupBrokerFixture(temp.root(), 1, "orders", 3, clock, 100);
                    RpcClient client = broker.client()) {
                assertEquals(1, join(client, "workers", "a").generation());
                GroupAssignment joined = join(client, "workers", "b");
                assertEquals(2, joined.generation());

                clock.advanceMillis(99);
                assertEquals(
                        ErrorCode.ILLEGAL_GENERATION,
                        client.call(
                                        new Messages.HeartbeatRequest(
                                                new GroupToken("workers", "a", 1)))
                                .error());
                assertEquals(
                        ErrorCode.ILLEGAL_GENERATION,
                        client.call(
                                        new Messages.HeartbeatRequest(
                                                new GroupToken("workers", "a", 99)))
                                .error());
                assertEquals(
                        ErrorCode.UNKNOWN_MEMBER,
                        client.call(
                                        new Messages.HeartbeatRequest(
                                                new GroupToken("workers", "missing", 2)))
                                .error());
                assertEquals(
                        ErrorCode.NONE,
                        client.call(
                                        new Messages.HeartbeatRequest(
                                                new GroupToken("workers", "b", 2)))
                                .error());
                assertEquals(Set.of(), broker.expireGroups(), "no member expires at 1099");

                clock.advanceMillis(1);
                assertEquals(
                        List.of("workers/a"),
                        new ArrayList<>(broker.expireGroups()),
                        "a expires exactly at 1100; failed heartbeats did not refresh it");
                GroupAssignment afterExpiry = assignment(client, "workers", "b");
                assertEquals(3, afterExpiry.generation());
                assertEquals(
                        Map.of(
                                "b",
                                List.of(
                                        new TopicPartition("orders", 0),
                                        new TopicPartition("orders", 1),
                                        new TopicPartition("orders", 2))),
                        afterExpiry.assignments());
                assertEquals(
                        ErrorCode.UNKNOWN_MEMBER,
                        client.call(
                                        new Messages.HeartbeatRequest(
                                                new GroupToken("workers", "a", 3)))
                                .error());
            }
        }
    }

    @Test
    @DisplayName("Idempotent join refreshes heartbeat but rejected join does not")
    void idempotentJoinRefreshesHeartbeatButRejectedJoinDoesNot() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker =
                            new GroupBrokerFixture(temp.root(), 1, "orders", 2, clock, 100);
                    RpcClient client = broker.client()) {
                assertEquals(1, join(client, "workers", "a").generation());
                assertEquals(2, join(client, "workers", "b").generation());

                clock.advanceMillis(90);
                assertEquals(
                        2,
                        join(client, "workers", "a").generation(),
                        "idempotent join refreshes heartbeat without advancing generation");
                assertEquals(
                        ErrorCode.INVALID_REQUEST,
                        client.call(new Messages.JoinGroupRequest("workers", "b", "other-topic"))
                                .error());

                clock.advanceMillis(10);
                assertEquals(
                        List.of("workers/b"),
                        new ArrayList<>(broker.expireGroups()),
                        "the rejected join does not extend b's original deadline");
                GroupAssignment afterBExpires = assignment(client, "workers", "a");
                assertEquals(3, afterBExpires.generation());
                assertEquals(
                        Map.of(
                                "a",
                                List.of(
                                        new TopicPartition("orders", 0),
                                        new TopicPartition("orders", 1))),
                        afterBExpires.assignments());

                clock.advanceMillis(89);
                assertEquals(
                        Set.of(),
                        broker.expireGroups(),
                        "a remains alive until 100 ms after its successful idempotent join");
                clock.advanceMillis(1);
                assertEquals(List.of("workers/a"), new ArrayList<>(broker.expireGroups()));
                assertEquals(
                        5,
                        join(client, "workers", "replacement").generation(),
                        "the empty group's generation survives expiration");
            }
        }
    }

    @Test
    @DisplayName("Expiration orders all identifiers and advances each changed group once")
    void expirationOrdersAllIdentifiersAndAdvancesEachChangedGroupOnce() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker =
                            new GroupBrokerFixture(temp.root(), 1, "orders", 3, clock, 100);
                    RpcClient client = broker.client()) {
                assertEquals(1, join(client, "zeta", "c").generation());
                assertEquals(2, join(client, "zeta", "a").generation());
                assertEquals(1, join(client, "alpha", "b").generation());
                assertEquals(2, join(client, "alpha", "c").generation());
                assertEquals(3, join(client, "alpha", "a").generation());

                clock.advanceMillis(100);
                assertEquals(
                        List.of("alpha/a", "alpha/b", "alpha/c", "zeta/a", "zeta/c"),
                        new ArrayList<>(broker.expireGroups()));
                assertEquals(Set.of(), broker.expireGroups(), "a repeated expiry is a no-op");
                assertEquals(
                        5,
                        join(client, "alpha", "new-member").generation(),
                        "three expired members change alpha's generation only once");
                assertEquals(
                        4,
                        join(client, "zeta", "new-member").generation(),
                        "two expired members change zeta's generation only once");
            }
        }
    }

    @Test
    @DisplayName("Explicitly left member cannot heartbeat or expire again")
    void explicitlyLeftMemberCannotHeartbeatOrExpireAgain() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            ManualTimeSource clock = new ManualTimeSource(1_000);
            try (GroupBrokerFixture broker =
                            new GroupBrokerFixture(temp.root(), 1, "orders", 1, clock, 100);
                    RpcClient client = broker.client()) {
                assertEquals(1, join(client, "workers", "a").generation());

                Messages.Reply leaveReply =
                        client.call(new Messages.LeaveGroupRequest("workers", "a"));
                assertEquals(ErrorCode.NONE, leaveReply.error());
                GroupAssignment afterLeave = ((Messages.GroupBody) leaveReply.body()).assignment();
                assertEquals(2, afterLeave.generation());
                assertEquals(Map.of(), afterLeave.assignments());

                assertEquals(
                        ErrorCode.UNKNOWN_MEMBER,
                        client.call(
                                        new Messages.HeartbeatRequest(
                                                new GroupToken("workers", "a", 2)))
                                .error());
                clock.setMillis(1_100);
                assertEquals(Set.of(), broker.expireGroups());

                GroupAssignment replacement = join(client, "workers", "b");
                assertEquals(3, replacement.generation());
                assertEquals(
                        Map.of("b", List.of(new TopicPartition("orders", 0))),
                        replacement.assignments());
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
