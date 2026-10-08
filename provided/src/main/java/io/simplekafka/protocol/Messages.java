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

/**
 * Value types for requests and responses in the course teaching protocol.
 *
 * <p>MessageCodec encodes each record's components in declaration order. A response error code is
 * carried in the frame header, with the error message represented by {@link ErrorBody}. Each record
 * documents any constructor validation or defensive copy it performs; other components are not
 * validated by construction.
 */
public final class Messages {
    private Messages() {}

    /** A sealed request value whose API id selects its course-protocol message schema. */
    public sealed interface Request
            permits MetadataRequest,
                    ProduceRequest,
                    FetchRequest,
                    CommitOffsetRequest,
                    FetchOffsetRequest,
                    JoinGroupRequest,
                    HeartbeatRequest,
                    LeaveGroupRequest,
                    GroupAssignmentRequest,
                    ReplicaFetchRequest {
        /**
         * Returns the course-protocol API id for this request type.
         *
         * @return the API id used to encode and dispatch this request
         */
        short apiId();
    }

    /** A successful response body or a protocol error message body. */
    public sealed interface Response
            permits MetadataBody,
                    ProduceBody,
                    FetchBody,
                    EmptyBody,
                    OffsetBody,
                    GroupBody,
                    ReplicaFetchBody,
                    ErrorBody {}

    /**
     * A response frame's error code paired with its successful body or error message.
     *
     * @param error frame error code; {@link ErrorCode#NONE} denotes success
     * @param body successful response body for {@code NONE}, otherwise an {@link ErrorBody}
     */
    public record Reply(ErrorCode error, Response body) {
        /**
         * Creates a reply whose body kind matches its frame error code.
         *
         * @param error frame error code
         * @param body successful response body for {@code NONE}, otherwise an error body
         * @throws NullPointerException if {@code error} or {@code body} is null
         * @throws IllegalArgumentException if {@code error} is {@link ErrorCode#NONE} and {@code
         *     body} is an {@link ErrorBody}, or if {@code error} is not {@code NONE} and {@code
         *     body} is not an {@link ErrorBody}
         */
        public Reply {
            Objects.requireNonNull(error);
            Objects.requireNonNull(body);
            if ((error == ErrorCode.NONE) == (body instanceof ErrorBody))
                throw new IllegalArgumentException(
                        "success and error body must match frame error code");
        }

        /**
         * Creates a success reply with the {@link ErrorCode#NONE} frame error.
         *
         * @param body successful response body
         * @return a reply containing {@code body} and {@code NONE}
         * @throws NullPointerException if {@code body} is null
         * @throws IllegalArgumentException if {@code body} is an {@link ErrorBody}
         */
        public static Reply success(Response body) {
            return new Reply(ErrorCode.NONE, body);
        }

        /**
         * Creates a failure reply with its message in an {@link ErrorBody}.
         *
         * @param error non-{@link ErrorCode#NONE} frame error code
         * @param message error message, which may be empty
         * @return a reply containing the error code and corresponding error body
         * @throws IllegalArgumentException if {@code error} is {@link ErrorCode#NONE}
         * @throws NullPointerException if {@code error} is null, or if {@code message} is null for
         *     a non-{@code NONE} error
         */
        public static Reply failure(ErrorCode error, String message) {
            if (error == ErrorCode.NONE)
                throw new IllegalArgumentException("failure requires a non-NONE code");
            return new Reply(error, new ErrorBody(message));
        }
    }

    /**
     * Request for metadata about a topic.
     *
     * <p>This record does not validate its component in the constructor.
     *
     * @param topic topic whose partition metadata is requested
     */
    public record MetadataRequest(String topic) implements Request {
        /**
         * Returns the API id for a metadata request.
         *
         * @return the API identifier {@link Api#METADATA}
         */
        @Override
        public short apiId() {
            return Api.METADATA;
        }
    }

    /**
     * Request to append a batch of records to a topic-partition.
     *
     * <p>The constructor copies the list structure into an immutable list; it does not validate the
     * other components.
     *
     * @param tp target topic-partition
     * @param epoch leader epoch expected by the broker
     * @param acks acknowledgment mode requested for the append
     * @param timeoutMillis acknowledgment timeout in milliseconds
     * @param records records to append in list order
     */
    public record ProduceRequest(
            TopicPartition tp, int epoch, Acks acks, long timeoutMillis, List<RecordData> records)
            implements Request {
        /**
         * Copies the records list for this request.
         *
         * @param tp target topic-partition
         * @param epoch leader epoch expected by the broker
         * @param acks acknowledgment mode requested for the append
         * @param timeoutMillis acknowledgment timeout in milliseconds
         * @param records records to append in list order
         * @throws NullPointerException if {@code records} or any list element is null
         */
        public ProduceRequest {
            records = List.copyOf(records);
        }

        /**
         * Returns the API id for a produce request.
         *
         * @return the API identifier {@link Api#PRODUCE}
         */
        @Override
        public short apiId() {
            return Api.PRODUCE;
        }
    }

    /**
     * Request to fetch consumer-visible records from a topic-partition.
     *
     * <p>{@code maxBytes} counts complete disk-record bytes including each record's length prefix.
     * A null token represents a request without group-membership fencing. The constructor does not
     * validate these components; ordinary consumer fetches expose only offsets below the
     * high-watermark.
     *
     * @param tp topic-partition to fetch
     * @param epoch leader epoch expected by the broker
     * @param offset first requested logical offset
     * @param maxRecords maximum number of records requested
     * @param maxBytes maximum total disk-record bytes requested
     * @param token optional group-membership token for fenced consumption
     */
    public record FetchRequest(
            TopicPartition tp,
            int epoch,
            long offset,
            int maxRecords,
            int maxBytes,
            GroupToken token)
            implements Request {
        /**
         * Returns the API id for a fetch request.
         *
         * @return the API identifier {@link Api#FETCH}
         */
        @Override
        public short apiId() {
            return Api.FETCH;
        }
    }

    /**
     * Request to commit a group's next offset for a topic-partition.
     *
     * <p>The next offset identifies the next record to process and may be moved backward. A null
     * token omits group-membership fencing; the record constructor does not validate its
     * components.
     *
     * @param key group and topic-partition whose progress is committed
     * @param nextOffset offset of the next record to process
     * @param token optional group-membership token
     */
    public record CommitOffsetRequest(OffsetKey key, long nextOffset, GroupToken token)
            implements Request {
        /**
         * Returns the API id for an offset-commit request.
         *
         * @return the API identifier {@link Api#COMMIT_OFFSET}
         */
        @Override
        public short apiId() {
            return Api.COMMIT_OFFSET;
        }
    }

    /**
     * Request for the committed next offset associated with a group and topic-partition.
     *
     * @param key group and topic-partition whose progress is queried
     */
    public record FetchOffsetRequest(OffsetKey key) implements Request {
        /**
         * Returns the API id for an offset-fetch request.
         *
         * @return the API identifier {@link Api#FETCH_OFFSET}
         */
        @Override
        public short apiId() {
            return Api.FETCH_OFFSET;
        }
    }

    /**
     * Request to join a topic's consumer group.
     *
     * <p>Group and member identifiers use the course's non-empty, 1-to-255-byte UTF-8 rule, not the
     * topic identifier's ASCII character restriction. The record constructor does not validate
     * them.
     *
     * @param group group identifier
     * @param member member identifier joining the group
     * @param topic topic whose partitions are assigned
     */
    public record JoinGroupRequest(String group, String member, String topic) implements Request {
        /**
         * Returns the API id for a group-join request.
         *
         * @return the API identifier {@link Api#JOIN_GROUP}
         */
        @Override
        public short apiId() {
            return Api.JOIN_GROUP;
        }
    }

    /**
     * Request to refresh a member's group-session lease.
     *
     * <p>The token may be null in this value type, although the broker rejects a heartbeat without
     * one.
     *
     * @param token group-membership token for the heartbeat
     */
    public record HeartbeatRequest(GroupToken token) implements Request {
        /**
         * Returns the API id for a heartbeat request.
         *
         * @return the API identifier {@link Api#HEARTBEAT}
         */
        @Override
        public short apiId() {
            return Api.HEARTBEAT;
        }
    }

    /**
     * Request for a group member to leave its group.
     *
     * <p>Group and member identifiers use the course's non-empty, 1-to-255-byte UTF-8 rule, not the
     * topic identifier's ASCII character restriction. The record constructor does not validate
     * them.
     *
     * @param group group identifier
     * @param member member identifier leaving the group
     */
    public record LeaveGroupRequest(String group, String member) implements Request {
        /**
         * Returns the API id for a group-leave request.
         *
         * @return the API identifier {@link Api#LEAVE_GROUP}
         */
        @Override
        public short apiId() {
            return Api.LEAVE_GROUP;
        }
    }

    /**
     * Request for the current assignment of one group member.
     *
     * <p>Group and member identifiers use the course's non-empty, 1-to-255-byte UTF-8 rule, not the
     * topic identifier's ASCII character restriction. The record constructor does not validate
     * them.
     *
     * @param group group identifier
     * @param member member whose assignment is requested
     */
    public record GroupAssignmentRequest(String group, String member) implements Request {
        /**
         * Returns the API id for a group-assignment request.
         *
         * @return the API identifier {@link Api#GROUP_ASSIGNMENT}
         */
        @Override
        public short apiId() {
            return Api.GROUP_ASSIGNMENT;
        }
    }

    /**
     * Request for a follower to fetch records from a leader replica.
     *
     * <p>The byte limit counts complete disk-record bytes including each length prefix. A recovery
     * read may inspect the leader's uncommitted suffix for reconciliation and is not a replication
     * progress report.
     *
     * @param tp topic-partition replicated by the requester
     * @param brokerId follower broker identifier
     * @param epoch leader epoch expected by the follower
     * @param fetchOffset first requested logical offset
     * @param maxRecords maximum number of records requested
     * @param maxBytes maximum total disk-record bytes requested
     * @param recoveryRead whether this is a reconciliation read rather than a progress fetch
     */
    public record ReplicaFetchRequest(
            TopicPartition tp,
            int brokerId,
            int epoch,
            long fetchOffset,
            int maxRecords,
            int maxBytes,
            boolean recoveryRead)
            implements Request {
        /**
         * Returns the API id for a replica-fetch request.
         *
         * @return the API identifier {@link Api#REPLICA_FETCH}
         */
        @Override
        public short apiId() {
            return Api.REPLICA_FETCH;
        }
    }

    /**
     * Successful metadata response containing partition metadata.
     *
     * @param partitions metadata entries, copied into an immutable list
     */
    public record MetadataBody(List<PartitionMetadata> partitions) implements Response {
        /**
         * Copies the partition metadata list into an immutable list.
         *
         * @param partitions partition metadata entries
         * @throws NullPointerException if {@code partitions} or any list element is null
         */
        public MetadataBody {
            partitions = List.copyOf(partitions);
        }
    }

    /**
     * Successful produce response containing the appended offset range.
     *
     * @param result append result returned for the batch
     */
    public record ProduceBody(AppendResult result) implements Response {
        /**
         * Creates a produce body with a non-null append result.
         *
         * @param result append result returned for the batch
         * @throws NullPointerException if {@code result} is null
         */
        public ProduceBody {
            Objects.requireNonNull(result);
        }
    }

    /**
     * Successful consumer-fetch response and the log boundaries used to serve it.
     *
     * <p>The constructor copies only the list structure and does not validate the numeric
     * boundaries.
     *
     * @param records fetched log records, in offset order
     * @param logStartOffset first retained log offset
     * @param logEndOffset log end offset, the next offset available for append
     * @param highWatermark committed boundary; ordinary consumers see offsets below it
     * @param epoch current leader epoch
     */
    public record FetchBody(
            List<LogRecord> records,
            long logStartOffset,
            long logEndOffset,
            long highWatermark,
            int epoch)
            implements Response {
        /**
         * Copies the fetched-record list into an immutable list.
         *
         * @param records fetched log records
         * @param logStartOffset first retained log offset
         * @param logEndOffset log end offset
         * @param highWatermark committed boundary
         * @param epoch current leader epoch
         * @throws NullPointerException if {@code records} or any list element is null
         */
        public FetchBody {
            records = List.copyOf(records);
        }
    }

    /** Successful response body for operations with no response fields. */
    public record EmptyBody() implements Response {}

    /**
     * Successful response containing an optional committed next offset.
     *
     * <p>An empty value means that no offset has been committed for the key; a consumer resuming
     * without a commit starts at offset zero.
     *
     * @param nextOffset committed next offset, or empty when no commit exists
     */
    public record OffsetBody(OptionalLong nextOffset) implements Response {
        /**
         * Creates an offset body with a non-null optional result.
         *
         * @param nextOffset committed next offset, or empty when no commit exists
         * @throws NullPointerException if {@code nextOffset} is null
         */
        public OffsetBody {
            Objects.requireNonNull(nextOffset);
        }
    }

    /**
     * Successful response containing a group assignment.
     *
     * @param assignment assignment returned for the group operation
     */
    public record GroupBody(GroupAssignment assignment) implements Response {
        /**
         * Creates a group body with a non-null assignment.
         *
         * @param assignment assignment returned for the group operation
         * @throws NullPointerException if {@code assignment} is null
         */
        public GroupBody {
            Objects.requireNonNull(assignment);
        }
    }

    /**
     * Successful replica-fetch response containing records and leader progress boundaries.
     *
     * <p>The records list is copied into an immutable list but its other components are not
     * validated. The returned records may extend beyond the high-watermark up to the leader's log
     * end offset.
     *
     * @param records fetched log records, in offset order
     * @param epoch current leader epoch
     * @param highWatermark committed boundary on the leader
     * @param leaderLogEndOffset leader's next offset available for append
     */
    public record ReplicaFetchBody(
            List<LogRecord> records, int epoch, long highWatermark, long leaderLogEndOffset)
            implements Response {
        /**
         * Copies the fetched-record list into an immutable list.
         *
         * @param records fetched log records
         * @param epoch current leader epoch
         * @param highWatermark committed boundary on the leader
         * @param leaderLogEndOffset leader's log end offset
         * @throws NullPointerException if {@code records} or any list element is null
         */
        public ReplicaFetchBody {
            records = List.copyOf(records);
        }
    }

    /**
     * Response body carrying the message for a failed request.
     *
     * <p>The message is non-null but may be empty; the error code remains in the frame header.
     *
     * @param message error message
     */
    public record ErrorBody(String message) implements Response {
        /**
         * Creates an error body with a non-null message.
         *
         * @param message error message, which may be empty
         * @throws NullPointerException if {@code message} is null
         */
        public ErrorBody {
            Objects.requireNonNull(message);
        }
    }
}
