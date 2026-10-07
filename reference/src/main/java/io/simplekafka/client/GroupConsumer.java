package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/** Same-topic group consumer with heartbeat fencing; owns a direct RpcClient and borrows a shared MetadataRouter. */
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

    public void subscribe(String topic) {
        ensureOpen();
        try {
            new TopicPartition(topic, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid subscription topic", exception);
        }
        if (router != null) router.refresh(topic);
        Messages.JoinGroupRequest request = new Messages.JoinGroupRequest(group, member, topic);
        GroupAssignment assignment = ClientSupport.requireBody(controlCall(request), Messages.GroupBody.class).assignment();
        List<TopicPartition> assigned = assignment.assignments().get(member);
        if (assigned == null) throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "member is not present in returned group assignment");
        this.topic = topic;
        this.token = new GroupToken(group, member, assignment.generation());
        consumer.setGroupToken(token);
        consumer.applyGroupAssignment(assigned);
    }

    public Map<TopicPartition, List<LogRecord>> poll(int maxRecords, int maxBytes) {
        ensureSubscribed();
        try {
            ClientSupport.requireSuccess(controlCall(new Messages.HeartbeatRequest(token)));
        } catch (CourseException exception) {
            if (exception.code() != ErrorCode.ILLEGAL_GENERATION) throw exception;
            refreshAssignment();
        }
        consumer.setGroupToken(token);
        return consumer.poll(maxRecords, maxBytes);
    }

    public void commitSync() {
        ensureSubscribed();
        consumer.setGroupToken(token);
        consumer.commitSync();
    }

    private void refreshAssignment() {
        Messages.GroupAssignmentRequest request = new Messages.GroupAssignmentRequest(group, member);
        GroupAssignment assignment = ClientSupport.requireBody(controlCall(request), Messages.GroupBody.class).assignment();
        List<TopicPartition> assigned = assignment.assignments().get(member);
        if (assigned == null) throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "member is not present in current group assignment");
        token = new GroupToken(group, member, assignment.generation());
        consumer.applyGroupAssignment(assigned);
        consumer.setGroupToken(token);
    }

    private Messages.Reply controlCall(Messages.Request request) {
        return consumer.controlCall(request);
    }

    private static GroupToken validateGroup(String group, String member) {
        try {
            return new GroupToken(group, member, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid group/member identifier", exception);
        }
    }

    private void ensureSubscribed() {
        ensureOpen();
        if (token == null || topic == null) throw new CourseException(ErrorCode.INVALID_REQUEST, "subscribe must be called first");
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("group consumer is closed");
    }

    @Override public void close() {
        if (closed) return;
        closed = true;
        consumer.close();
        token = null;
        topic = null;
    }
}
