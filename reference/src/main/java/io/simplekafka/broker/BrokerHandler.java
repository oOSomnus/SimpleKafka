package io.simplekafka.broker;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.replication.ReplicaFetchBackend;

import java.util.List;
import java.util.Objects;

/** Request dispatch shared by single and replicated brokers. */
public final class BrokerHandler {
    private final PartitionCatalog catalog;
    private final ClusterAuthority authority;
    private final GroupCoordinator groups;
    private final OffsetStore offsets;

    public BrokerHandler(
            PartitionCatalog catalog,
            ClusterAuthority authority,
            GroupCoordinator groups,
            OffsetStore offsets) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.authority = authority;
        this.groups = groups;
        this.offsets = offsets;
    }

    /** Dispatches one decoded request and converts domain failures to protocol replies. */
    public Messages.Reply handle(Messages.Request request) {
        try {
            Objects.requireNonNull(request, "request");
            if (request instanceof Messages.MetadataRequest metadata) return metadata(metadata);
            if (request instanceof Messages.ProduceRequest produce) return produce(produce);
            if (request instanceof Messages.FetchRequest fetch) return fetch(fetch);
            if (request instanceof Messages.CommitOffsetRequest commit) return commit(commit);
            if (request instanceof Messages.FetchOffsetRequest fetchOffset)
                return fetchOffset(fetchOffset);
            if (request instanceof Messages.JoinGroupRequest join) return join(join);
            if (request instanceof Messages.HeartbeatRequest heartbeat) return heartbeat(heartbeat);
            if (request instanceof Messages.LeaveGroupRequest leave) return leave(leave);
            if (request instanceof Messages.GroupAssignmentRequest assignment)
                return assignment(assignment);
            if (request instanceof Messages.ReplicaFetchRequest replicaFetch)
                return replicaFetch(replicaFetch);
            throw new CourseException(ErrorCode.INVALID_REQUEST, "unsupported request");
        } catch (ExerciseNotImplementedException exception) {
            throw exception;
        } catch (CourseException exception) {
            return Messages.Reply.failure(exception.code(), safeMessage(exception));
        } catch (IllegalArgumentException | NullPointerException exception) {
            return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, safeMessage(exception));
        } catch (RuntimeException exception) {
            return Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "broker request failed");
        }
    }

    private Messages.Reply metadata(Messages.MetadataRequest request) {
        if (request.topic() == null) throw invalid("topic is required");
        List<PartitionMetadata> result =
                authority == null
                        ? catalog.metadata(request.topic())
                        : authority.metadata(request.topic());
        return Messages.Reply.success(new Messages.MetadataBody(result));
    }

    private Messages.Reply produce(Messages.ProduceRequest request) {
        TopicPartition tp = requirePartition(request.tp());
        if (request.acks() == null || request.records() == null || request.timeoutMillis() < 0)
            throw invalid("invalid produce parameters");
        if (request.records().isEmpty()) throw invalid("produce batch must not be empty");
        PartitionBackend backend = catalog.backend(tp);
        requireLeader(tp, request.epoch(), backend);
        AppendResult result =
                backend.produce(
                        request.records(),
                        request.acks(),
                        request.epoch(),
                        request.timeoutMillis());
        return Messages.Reply.success(new Messages.ProduceBody(result));
    }

    private Messages.Reply fetch(Messages.FetchRequest request) {
        TopicPartition tp = requirePartition(request.tp());
        if (request.maxRecords() <= 0 || request.maxBytes() <= 0)
            throw invalid("fetch limits must be positive");
        PartitionBackend backend = catalog.backend(tp);
        requireLeader(tp, request.epoch(), backend);
        if (request.token() != null) {
            requireGroups();
            groups.validate(request.token(), tp);
        }
        List<LogRecord> records =
                backend.fetch(
                        request.offset(),
                        request.maxRecords(),
                        request.maxBytes(),
                        request.epoch());
        if (authority == null) {
            return Messages.Reply.success(
                    new Messages.FetchBody(
                            records,
                            backend.logStartOffset(),
                            backend.logEndOffset(),
                            backend.highWatermark(),
                            backend.epoch()));
        }
        ClusterAuthority.PartitionState state = authority.partitionState(tp);
        int leaderId;
        int currentEpoch;
        long highWatermark;
        state.lock.lock();
        try {
            if (request.epoch() != state.epoch)
                throw new CourseException(ErrorCode.FENCED_EPOCH, "stale partition epoch");
            leaderId = state.leaderId;
            if (leaderId != catalog.brokerId())
                throw new CourseException(ErrorCode.NOT_LEADER, "broker is not the current leader");
            currentEpoch = state.epoch;
            highWatermark = state.highWatermark;
        } finally {
            state.lock.unlock();
        }
        if (!authority.isOnline(leaderId))
            throw new CourseException(ErrorCode.NOT_LEADER, "broker is not the current leader");
        return Messages.Reply.success(
                new Messages.FetchBody(
                        records,
                        backend.logStartOffset(),
                        backend.logEndOffset(),
                        highWatermark,
                        currentEpoch));
    }

    private Messages.Reply commit(Messages.CommitOffsetRequest request) {
        OffsetKey key = Objects.requireNonNull(request.key(), "offset key");
        OffsetStore store = requireOffsets();
        if (request.token() == null) {
            store.commit(key, request.nextOffset());
        } else {
            requireGroups();
            groups.commitOffset(request.token(), key.tp(), key, request.nextOffset(), store);
        }
        return Messages.Reply.success(new Messages.EmptyBody());
    }

    private Messages.Reply fetchOffset(Messages.FetchOffsetRequest request) {
        OffsetKey key = Objects.requireNonNull(request.key(), "offset key");
        return Messages.Reply.success(new Messages.OffsetBody(requireOffsets().fetch(key)));
    }

    private Messages.Reply join(Messages.JoinGroupRequest request) {
        requireGroups();
        GroupAssignment assignment =
                groups.join(request.group(), request.member(), request.topic());
        return Messages.Reply.success(new Messages.GroupBody(assignment));
    }

    private Messages.Reply heartbeat(Messages.HeartbeatRequest request) {
        requireGroups();
        groups.heartbeat(Objects.requireNonNull(request.token(), "group token"));
        return Messages.Reply.success(new Messages.EmptyBody());
    }

    private Messages.Reply leave(Messages.LeaveGroupRequest request) {
        requireGroups();
        return Messages.Reply.success(
                new Messages.GroupBody(groups.leave(request.group(), request.member())));
    }

    private Messages.Reply assignment(Messages.GroupAssignmentRequest request) {
        requireGroups();
        return Messages.Reply.success(
                new Messages.GroupBody(groups.assignment(request.group(), request.member())));
    }

    private Messages.Reply replicaFetch(Messages.ReplicaFetchRequest request) {
        if (authority == null) throw invalid("replica fetch is unavailable on a single broker");
        TopicPartition tp = requirePartition(request.tp());
        ClusterAuthority.PartitionState state = authority.partitionState(tp);
        state.lock.lock();
        try {
            if (request.epoch() != state.epoch)
                throw new CourseException(
                        ErrorCode.FENCED_EPOCH, "replica fetch uses a stale epoch");
            if (state.leaderId != catalog.brokerId())
                throw new CourseException(
                        ErrorCode.NOT_LEADER, "replica fetch must be served by the leader");
        } finally {
            state.lock.unlock();
        }
        PartitionBackend backend = catalog.backend(tp);
        if (!(backend instanceof ReplicaFetchBackend replicaBackend))
            throw invalid("partition backend does not support replica fetch");
        Messages.ReplicaFetchBody result =
                replicaBackend.fetchForReplica(
                        request.brokerId(),
                        request.epoch(),
                        request.fetchOffset(),
                        request.maxRecords(),
                        request.maxBytes(),
                        request.recoveryRead());
        return Messages.Reply.success(result);
    }

    private void requireLeader(TopicPartition tp, int epoch, PartitionBackend backend) {
        if (authority == null) {
            if (epoch != backend.epoch())
                throw new CourseException(ErrorCode.FENCED_EPOCH, "stale partition epoch");
            return;
        }
        ClusterAuthority.PartitionState state = authority.partitionState(tp);
        int leaderId;
        state.lock.lock();
        try {
            if (epoch != state.epoch)
                throw new CourseException(ErrorCode.FENCED_EPOCH, "stale partition epoch");
            leaderId = state.leaderId;
            if (leaderId != catalog.brokerId())
                throw new CourseException(ErrorCode.NOT_LEADER, "broker is not the current leader");
        } finally {
            state.lock.unlock();
        }
        if (!authority.isOnline(leaderId))
            throw new CourseException(ErrorCode.NOT_LEADER, "broker is not the current leader");
    }

    private TopicPartition requirePartition(TopicPartition tp) {
        if (tp == null) throw invalid("topic partition is required");
        return tp;
    }

    private GroupCoordinator requireGroups() {
        if (groups == null) throw invalid("group coordination is unavailable");
        return groups;
    }

    private OffsetStore requireOffsets() {
        if (offsets == null) throw invalid("offset storage is unavailable");
        return offsets;
    }

    private static CourseException invalid(String message) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message);
    }

    private static String safeMessage(Throwable failure) {
        String message = failure.getMessage();
        return message == null || message.isBlank() ? failure.getClass().getSimpleName() : message;
    }
}
