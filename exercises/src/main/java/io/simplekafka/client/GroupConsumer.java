package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;

import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Same-topic group consumer with heartbeat fencing; owns a direct RpcClient and borrows a shared
 * MetadataRouter.
 */
public final class GroupConsumer implements AutoCloseable {
    private final MetadataRouter router;
    private final String group;
    private final String member;
    private final SimpleConsumer consumer;
    private String topic;
    private GroupToken token;
    private boolean closed;

    public GroupConsumer(RpcClient client, String group, String member) {
        this.router = null;
        this.group = validateGroup(group, member).group();
        this.member = member;
        this.consumer = new SimpleConsumer(Objects.requireNonNull(client, "client"), group);
    }

    public GroupConsumer(MetadataRouter router, String group, String member) {
        this.router = Objects.requireNonNull(router, "router");
        this.group = validateGroup(group, member).group();
        this.member = member;
        this.consumer = new SimpleConsumer(router, group);
    }

    /**
     * Step 20: join this same-topic group and establish the member's assignment/token. Contract:
     * subscribe is the explicit way to rejoin after UNKNOWN_MEMBER. Test: Step20Test. Lesson:
     * docs/book/chapters/05-consumer-groups.tex, Step 20.
     */
    public void subscribe(String topic) {
        throw new ExerciseNotImplementedException(20, "subscribe");
    }

    /**
     * Step 20: heartbeat, recover stale generations from the current assignment, then fetch with
     * token. Contract: do not auto-rejoin an UNKNOWN_MEMBER; discard revoked positions and resume
     * new ones. Test: Step20Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 20.
     */
    public Map<TopicPartition, List<LogRecord>> poll(int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(20, "poll");
    }

    /**
     * Step 20: commit positions with the current group token and server-side ownership fence.
     * Contract: a stale generation/non-owner commit leaves stored offsets unchanged. Test:
     * Step20Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 20.
     */
    public void commitSync() {
        throw new ExerciseNotImplementedException(20, "commitSync");
    }

    private Messages.Reply controlCall(Messages.Request request) {
        return consumer.controlCall(request);
    }

    private static GroupToken validateGroup(String group, String member) {
        try {
            return new GroupToken(group, member, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid group/member identifier", exception);
        }
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("group consumer is closed");
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        consumer.close();
        token = null;
        topic = null;
    }
}
