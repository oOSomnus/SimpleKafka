package io.simplekafka.group;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.TimeSource;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.ReentrantLock;

/** Single-process, same-topic group membership, assignment, heartbeat, and generation fence. */
public final class GroupCoordinator implements AutoCloseable {
    private final PartitionCatalog catalog;
    private final RoundRobinAssignor assignor;
    private final TimeSource time;
    private final long sessionTimeoutMillis;
    private final ReentrantLock lock = new ReentrantLock();
    private final Map<String, GroupState> groups = new TreeMap<>();
    private boolean closed;

    public GroupCoordinator(
            PartitionCatalog catalog,
            RoundRobinAssignor assignor,
            TimeSource time,
            long sessionTimeoutMillis) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.assignor = Objects.requireNonNull(assignor, "assignor");
        this.time = Objects.requireNonNull(time, "time");
        if (sessionTimeoutMillis <= 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "sessionTimeoutMillis must be positive");
        this.sessionTimeoutMillis = sessionTimeoutMillis;
    }

    public GroupAssignment join(String group, String member, String topic) {
        lock.lock();
        try {
            ensureOpen();
            validateIdentifiers(group, member, topic);
            GroupState state = groups.get(group);
            if (state != null && !state.topic.equals(topic)) {
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST,
                        "all members of a group must subscribe to the same topic");
            }
            MemberState existing = state == null ? null : state.members.get(member);
            if (existing != null) {
                existing.lastHeartbeat = time.nowMillis();
                return state.assignment;
            }

            List<TopicPartition> partitions = topicPartitions(topic);
            TreeMap<String, MemberState> nextMembers =
                    state == null ? new TreeMap<>() : new TreeMap<>(state.members);
            nextMembers.put(member, new MemberState(time.nowMillis()));
            int generation = nextGeneration(state == null ? 0 : state.generation);
            GroupAssignment nextAssignment = assign(generation, nextMembers, partitions);
            if (state == null) {
                state = new GroupState(topic);
                groups.put(group, state);
            }
            state.members.clear();
            state.members.putAll(nextMembers);
            state.generation = generation;
            state.assignment = nextAssignment;
            return state.assignment;
        } finally {
            lock.unlock();
        }
    }

    public GroupAssignment leave(String group, String member) {
        lock.lock();
        try {
            ensureOpen();
            validateGroupMember(group, member);
            GroupState state = groups.get(group);
            if (state == null || !state.members.containsKey(member))
                throw unknownMember(group, member);
            TreeMap<String, MemberState> nextMembers = new TreeMap<>(state.members);
            nextMembers.remove(member);
            List<TopicPartition> partitions =
                    nextMembers.isEmpty() ? List.of() : topicPartitions(state.topic);
            int generation = nextGeneration(state.generation);
            GroupAssignment nextAssignment = assign(generation, nextMembers, partitions);
            state.members.clear();
            state.members.putAll(nextMembers);
            state.generation = generation;
            state.assignment = nextAssignment;
            return state.assignment;
        } finally {
            lock.unlock();
        }
    }

    public GroupAssignment assignment(String group, String member) {
        lock.lock();
        try {
            ensureOpen();
            validateGroupMember(group, member);
            GroupState state = groups.get(group);
            if (state == null || !state.members.containsKey(member))
                throw unknownMember(group, member);
            return state.assignment;
        } finally {
            lock.unlock();
        }
    }

    public void heartbeat(GroupToken token) {
        lock.lock();
        try {
            ensureOpen();
            MemberState member = requireCurrentMember(token);
            member.lastHeartbeat = time.nowMillis();
        } finally {
            lock.unlock();
        }
    }

    public Set<String> expire() {
        lock.lock();
        try {
            ensureOpen();
            long now = time.nowMillis();
            LinkedHashSet<String> expired = new LinkedHashSet<>();
            for (Map.Entry<String, GroupState> groupEntry : groups.entrySet()) {
                String group = groupEntry.getKey();
                GroupState state = groupEntry.getValue();
                TreeMap<String, MemberState> nextMembers = new TreeMap<>(state.members);
                for (Map.Entry<String, MemberState> memberEntry : state.members.entrySet()) {
                    long lastHeartbeat = memberEntry.getValue().lastHeartbeat;
                    if (now >= lastHeartbeat && now - lastHeartbeat >= sessionTimeoutMillis) {
                        nextMembers.remove(memberEntry.getKey());
                        expired.add(group + "/" + memberEntry.getKey());
                    }
                }
                if (nextMembers.size() == state.members.size()) continue;
                List<TopicPartition> partitions =
                        nextMembers.isEmpty() ? List.of() : topicPartitions(state.topic);
                int generation = nextGeneration(state.generation);
                GroupAssignment nextAssignment = assign(generation, nextMembers, partitions);
                state.members.clear();
                state.members.putAll(nextMembers);
                state.generation = generation;
                state.assignment = nextAssignment;
            }
            return java.util.Collections.unmodifiableSet(expired);
        } finally {
            lock.unlock();
        }
    }

    public void validate(GroupToken token, TopicPartition tp) {
        lock.lock();
        try {
            ensureOpen();
            if (tp == null)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "topic-partition must not be null");
            requireCurrentMember(token);
            GroupState state = groups.get(token.group());
            List<TopicPartition> owned = state.assignment.assignments().get(token.member());
            if (owned == null || !owned.contains(tp)) {
                throw new CourseException(ErrorCode.NOT_ASSIGNED, "member does not own " + tp);
            }
        } finally {
            lock.unlock();
        }
    }

    /** Atomically fences a group commit with generation and ownership validation. */
    public void commitOffset(
            GroupToken token,
            TopicPartition tp,
            OffsetKey key,
            long nextOffset,
            OffsetStore store) {
        lock.lock();
        try {
            ensureOpen();
            if (tp == null || key == null || store == null)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST,
                        "topic-partition, offset key, and store are required");
            if (token == null || !token.group().equals(key.group()) || !tp.equals(key.tp())) {
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "commit token and offset key do not match");
            }
            validate(token, tp);
            store.commit(key, nextOffset);
        } finally {
            lock.unlock();
        }
    }

    private MemberState requireCurrentMember(GroupToken token) {
        if (token == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "group token must not be null");
        GroupState state = groups.get(token.group());
        if (state == null || !state.members.containsKey(token.member()))
            throw unknownMember(token.group(), token.member());
        if (token.generation() != state.generation) {
            throw new CourseException(
                    ErrorCode.ILLEGAL_GENERATION, "stale generation " + token.generation());
        }
        return state.members.get(token.member());
    }

    private List<TopicPartition> topicPartitions(String topic) {
        List<PartitionMetadata> metadata = catalog.metadata(topic);
        TreeSet<TopicPartition> partitions = new TreeSet<>();
        for (PartitionMetadata partition : metadata) partitions.add(partition.tp());
        if (partitions.isEmpty())
            throw new CourseException(
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "topic has no partitions: " + topic);
        return List.copyOf(partitions);
    }

    private GroupAssignment assign(
            int generation, Map<String, MemberState> members, List<TopicPartition> partitions) {
        List<String> memberIds = new ArrayList<>(members.keySet());
        Map<String, List<TopicPartition>> assignments =
                memberIds.isEmpty() ? Map.of() : assignor.assign(partitions, memberIds);
        return new GroupAssignment(generation, assignments);
    }

    private static int nextGeneration(int generation) {
        try {
            return Math.incrementExact(generation);
        } catch (ArithmeticException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "group generation overflow", exception);
        }
    }

    private static void validateIdentifiers(String group, String member, String topic) {
        validateGroupMember(group, member);
        try {
            new TopicPartition(topic, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid group topic", exception);
        }
    }

    private static void validateGroupMember(String group, String member) {
        try {
            new GroupToken(group, member, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid group/member identifier", exception);
        }
    }

    private static CourseException unknownMember(String group, String member) {
        return new CourseException(
                ErrorCode.UNKNOWN_MEMBER, "unknown group member: " + group + "/" + member);
    }

    private void ensureOpen() {
        if (closed) throw new IllegalStateException("group coordinator is closed");
    }

    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            groups.clear();
        } finally {
            lock.unlock();
        }
    }

    private static final class GroupState {
        private final String topic;
        private final TreeMap<String, MemberState> members = new TreeMap<>();
        private int generation;
        private GroupAssignment assignment = new GroupAssignment(0, Map.of());

        private GroupState(String topic) {
            this.topic = topic;
        }
    }

    private static final class MemberState {
        private long lastHeartbeat;

        private MemberState(long lastHeartbeat) {
            this.lastHeartbeat = lastHeartbeat;
        }
    }
}
