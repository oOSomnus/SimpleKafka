package io.simplekafka.protocol;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import java.util.List;
import java.util.Objects;
import java.util.OptionalLong;

public final class Messages {
    private Messages() {}

    public sealed interface Request permits MetadataRequest, ProduceRequest, FetchRequest,
            CommitOffsetRequest, FetchOffsetRequest, JoinGroupRequest, HeartbeatRequest,
            LeaveGroupRequest, GroupAssignmentRequest, ReplicaFetchRequest {
        short apiId();
    }
    public sealed interface Response permits MetadataBody, ProduceBody, FetchBody, EmptyBody,
            OffsetBody, GroupBody, ReplicaFetchBody, ErrorBody {}

    public record Reply(ErrorCode error, Response body) {
        public Reply {
            Objects.requireNonNull(error);
            Objects.requireNonNull(body);
            if ((error == ErrorCode.NONE) == (body instanceof ErrorBody))
                throw new IllegalArgumentException("success and error body must match frame error code");
        }
        public static Reply success(Response body) { return new Reply(ErrorCode.NONE, body); }
        public static Reply failure(ErrorCode error, String message) {
            if (error == ErrorCode.NONE) throw new IllegalArgumentException("failure requires a non-NONE code");
            return new Reply(error, new ErrorBody(message));
        }
    }
    public record MetadataRequest(String topic) implements Request { @Override public short apiId() { return Api.METADATA; } }
    public record ProduceRequest(TopicPartition tp, int epoch, Acks acks, long timeoutMillis, List<RecordData> records) implements Request {
        public ProduceRequest { records = List.copyOf(records); }
        @Override public short apiId() { return Api.PRODUCE; }
    }
    public record FetchRequest(TopicPartition tp, int epoch, long offset, int maxRecords, int maxBytes, GroupToken token) implements Request {
        @Override public short apiId() { return Api.FETCH; }
    }
    public record CommitOffsetRequest(OffsetKey key, long nextOffset, GroupToken token) implements Request {
        @Override public short apiId() { return Api.COMMIT_OFFSET; }
    }
    public record FetchOffsetRequest(OffsetKey key) implements Request { @Override public short apiId() { return Api.FETCH_OFFSET; } }
    public record JoinGroupRequest(String group, String member, String topic) implements Request { @Override public short apiId() { return Api.JOIN_GROUP; } }
    public record HeartbeatRequest(GroupToken token) implements Request { @Override public short apiId() { return Api.HEARTBEAT; } }
    public record LeaveGroupRequest(String group, String member) implements Request { @Override public short apiId() { return Api.LEAVE_GROUP; } }
    public record GroupAssignmentRequest(String group, String member) implements Request { @Override public short apiId() { return Api.GROUP_ASSIGNMENT; } }
    public record ReplicaFetchRequest(TopicPartition tp, int brokerId, int epoch, long fetchOffset,
                                      int maxRecords, int maxBytes, boolean recoveryRead) implements Request {
        @Override public short apiId() { return Api.REPLICA_FETCH; }
    }

    public record MetadataBody(List<PartitionMetadata> partitions) implements Response { public MetadataBody { partitions = List.copyOf(partitions); } }
    public record ProduceBody(AppendResult result) implements Response { public ProduceBody { Objects.requireNonNull(result); } }
    public record FetchBody(List<LogRecord> records, long logStartOffset, long logEndOffset,
                            long highWatermark, int epoch) implements Response {
        public FetchBody { records = List.copyOf(records); }
    }
    public record EmptyBody() implements Response {}
    public record OffsetBody(OptionalLong nextOffset) implements Response { public OffsetBody { Objects.requireNonNull(nextOffset); } }
    public record GroupBody(GroupAssignment assignment) implements Response { public GroupBody { Objects.requireNonNull(assignment); } }
    public record ReplicaFetchBody(List<LogRecord> records, int epoch, long highWatermark,
                                   long leaderLogEndOffset) implements Response {
        public ReplicaFetchBody { records = List.copyOf(records); }
    }
    public record ErrorBody(String message) implements Response { public ErrorBody { Objects.requireNonNull(message); } }
}
