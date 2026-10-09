package io.simplekafka.protocol;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.BusinessEvent;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.GroupAssignment;
import io.simplekafka.model.GroupToken;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

/**
 * Encodes and decodes request and response bodies for the course teaching protocol.
 *
 * <p>Body fields use the protocol's big-endian encoding. Frame headers are handled separately by
 * {@link FrameCodec}; successful responses carry their body here, while the error code is carried
 * in the frame header.
 */
public final class MessageCodec {
    private static final int MAX_FRAME_LENGTH = 8_388_608;
    private static final int FRAME_HEADER_LENGTH = 10;
    private static final int MAX_PAYLOAD_LENGTH = MAX_FRAME_LENGTH - FRAME_HEADER_LENGTH;
    public static final int MAX_FETCH_BYTE_BUDGET =
            MAX_PAYLOAD_LENGTH - Integer.BYTES - (3 * Long.BYTES + Integer.BYTES);
    public static final int MAX_REPLICA_FETCH_BYTE_BUDGET =
            MAX_PAYLOAD_LENGTH - Integer.BYTES - (2 * Long.BYTES + Integer.BYTES);
    private static final int MAX_RECORD_LENGTH = 1_048_576;
    private static final int RECORD_FIXED_LENGTH = 28;
    private static final int MAX_IDENTIFIER_BYTES = 255;

    private MessageCodec() {}

    /**
     * The key and next offset decoded from an OffsetStore value.
     *
     * @param key offset key stored in the value
     * @param nextOffset committed next offset
     */
    public record OffsetEntry(OffsetKey key, long nextOffset) {
        /**
         * Creates a decoded offset entry with a key and nonnegative next offset.
         *
         * @param key offset key stored in the value
         * @param nextOffset committed next offset
         * @throws CourseException if {@code key} is null or {@code nextOffset} is negative, with
         *     error code {@link ErrorCode#INVALID_REQUEST}
         */
        public OffsetEntry {
            if (key == null || nextOffset < 0) throw invalid("invalid offset entry");
        }
    }

    /**
     * Encodes a request as a course-protocol body, without a frame header.
     *
     * @param request request value to encode
     * @return a newly allocated byte array containing the encoded body
     * @throws CourseException if {@code request} is null, unsupported, invalid for encoding, or
     *     exceeds the frame payload limit
     */
    public static byte[] encodeRequest(Messages.Request request) {
        if (request == null) throw invalid("request is null");
        Writer sizing = new Writer(null);
        writeRequest(sizing, request);
        byte[] payload = new byte[sizing.position];
        Writer output = new Writer(payload);
        writeRequest(output, request);
        return payload;
    }

    /**
     * Decodes a course-protocol request body selected by API id.
     *
     * @param api course API identifier selecting the request type
     * @param payload encoded request body, without a frame header
     * @return the decoded request value
     * @throws CourseException if the API is unknown or the payload is null, too large, malformed,
     *     or contains trailing bytes
     */
    public static Messages.Request decodeRequest(short api, byte[] payload) {
        Api.requireKnown(api);
        Reader input = new Reader(payload);
        try {
            Messages.Request request =
                    switch (api) {
                        case Api.METADATA -> new Messages.MetadataRequest(input.readTopic());
                        case Api.PRODUCE -> readProduceRequest(input);
                        case Api.IDEMPOTENT_PRODUCE -> readIdempotentProduceRequest(input);
                        case Api.FETCH -> readFetchRequest(input);
                        case Api.COMMIT_OFFSET -> readCommitOffsetRequest(input);
                        case Api.FETCH_OFFSET ->
                                new Messages.FetchOffsetRequest(input.readOffsetKey());
                        case Api.JOIN_GROUP ->
                                new Messages.JoinGroupRequest(
                                        input.readIdentifier("group"),
                                        input.readIdentifier("member"),
                                        input.readTopic());
                        case Api.HEARTBEAT ->
                                new Messages.HeartbeatRequest(input.readOptionalToken());
                        case Api.LEAVE_GROUP ->
                                new Messages.LeaveGroupRequest(
                                        input.readIdentifier("group"),
                                        input.readIdentifier("member"));
                        case Api.GROUP_ASSIGNMENT ->
                                new Messages.GroupAssignmentRequest(
                                        input.readIdentifier("group"),
                                        input.readIdentifier("member"));
                        case Api.REPLICA_FETCH -> readReplicaFetchRequest(input);
                        default -> throw invalid("unknown API: " + api);
                    };
            input.requireEnd();
            return request;
        } catch (CourseException exception) {
            throw exception;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw invalid("invalid request body", exception);
        }
    }

    /**
     * Decodes a request while also validating the request-only frame error field.
     *
     * @param api course API identifier selecting the request type
     * @param frameError error value from the request frame header
     * @param payload encoded request body, without a frame header
     * @return the decoded request value
     * @throws CourseException if {@code frameError} is not {@link ErrorCode#NONE}, the API is
     *     unknown, or the payload is invalid
     */
    public static Messages.Request decodeRequest(short api, ErrorCode frameError, byte[] payload) {
        if (frameError != ErrorCode.NONE) throw invalid("request frame error must be NONE");
        return decodeRequest(api, payload);
    }

    /**
     * Caps fetch requests whose disk-byte budget could encode beyond one response frame.
     *
     * @param request request to cap if it is a fetch request
     * @return a request with a capped budget, or the same object if it is not capped
     */
    public static Messages.Request capFetchBudget(Messages.Request request) {
        if (request instanceof Messages.FetchRequest fetch
                && fetch.maxBytes() > MAX_FETCH_BYTE_BUDGET)
            return new Messages.FetchRequest(
                    fetch.tp(),
                    fetch.epoch(),
                    fetch.offset(),
                    fetch.maxRecords(),
                    MAX_FETCH_BYTE_BUDGET,
                    fetch.token());
        if (request instanceof Messages.ReplicaFetchRequest fetch
                && fetch.maxBytes() > MAX_REPLICA_FETCH_BYTE_BUDGET)
            return new Messages.ReplicaFetchRequest(
                    fetch.tp(),
                    fetch.brokerId(),
                    fetch.epoch(),
                    fetch.fetchOffset(),
                    fetch.maxRecords(),
                    MAX_REPLICA_FETCH_BYTE_BUDGET,
                    fetch.recoveryRead());
        return request;
    }

    /**
     * Encodes a response body for the course protocol, without a frame header.
     *
     * @param response response value to encode
     * @return a newly allocated byte array containing the encoded body
     * @throws CourseException if {@code response} is null, unsupported, invalid for encoding, or
     *     exceeds the frame payload limit
     */
    public static byte[] encodeReply(Messages.Response response) {
        if (response == null) throw invalid("response is null");
        Writer sizing = new Writer(null);
        writeReply(sizing, response);
        byte[] payload = new byte[sizing.position];
        Writer output = new Writer(payload);
        writeReply(output, response);
        return payload;
    }

    /**
     * Decodes either a successful response body or the error message body selected by a frame error
     * code.
     *
     * @param api course API identifier selecting the successful response type
     * @param frameError error value from the response frame header
     * @param payload encoded response or error body, without a frame header
     * @return the decoded success response or {@link Messages.ErrorBody}
     * @throws CourseException if the API is unknown, {@code frameError} is null, or the body is
     *     malformed
     */
    public static Messages.Response decodeReply(short api, ErrorCode frameError, byte[] payload) {
        Api.requireKnown(api);
        if (frameError == null) throw invalid("frame error is null");
        return frameError == ErrorCode.NONE ? decodeReply(api, payload) : decodeErrorBody(payload);
    }

    /**
     * Decodes a successful response body selected by API.
     *
     * @param api course API identifier selecting the response type
     * @param payload encoded successful response body, without a frame header
     * @return the decoded response value
     * @throws CourseException if the API is unknown or the payload is null, too large, malformed,
     *     or contains trailing bytes
     */
    public static Messages.Response decodeReply(short api, byte[] payload) {
        Api.requireKnown(api);
        Reader input = new Reader(payload);
        try {
            Messages.Response response =
                    switch (api) {
                        case Api.METADATA -> new Messages.MetadataBody(input.readMetadataList());
                        case Api.PRODUCE, Api.IDEMPOTENT_PRODUCE ->
                                new Messages.ProduceBody(input.readAppendResult());
                        case Api.FETCH -> readFetchBody(input);
                        case Api.COMMIT_OFFSET, Api.HEARTBEAT -> new Messages.EmptyBody();
                        case Api.FETCH_OFFSET -> new Messages.OffsetBody(input.readOptionalLong());
                        case Api.JOIN_GROUP, Api.LEAVE_GROUP, Api.GROUP_ASSIGNMENT ->
                                new Messages.GroupBody(input.readGroupAssignment());
                        case Api.REPLICA_FETCH -> readReplicaFetchBody(input);
                        default -> throw invalid("unknown API: " + api);
                    };
            input.requireEnd();
            return response;
        } catch (CourseException exception) {
            throw exception;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw invalid("invalid response body", exception);
        }
    }

    /**
     * Decodes the message carried in a failure frame; its error code remains in the frame header.
     *
     * @param payload encoded error body, without a frame header
     * @return the decoded error message body, including an empty message if present
     * @throws CourseException if the payload is null, too large, malformed, or contains trailing
     *     bytes
     */
    public static Messages.ErrorBody decodeErrorBody(byte[] payload) {
        Reader input = new Reader(payload);
        try {
            Messages.ErrorBody body =
                    new Messages.ErrorBody(
                            input.readString(MAX_PAYLOAD_LENGTH, true, "error message"));
            input.requireEnd();
            return body;
        } catch (CourseException exception) {
            throw exception;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw invalid("invalid error body", exception);
        }
    }

    /**
     * Encodes an offset key and next offset as an OffsetStore record value.
     *
     * @param key key identifying the group's topic-partition offset
     * @param nextOffset offset of the next record to process
     * @return encoded offset-entry bytes
     * @throws CourseException if {@code key} is null or {@code nextOffset} is negative, with error
     *     code {@link ErrorCode#INVALID_REQUEST}
     */
    public static byte[] encodeOffsetEntry(OffsetKey key, long nextOffset) {
        if (key == null || nextOffset < 0) throw invalid("invalid offset entry");
        Writer sizing = new Writer(null);
        sizing.writeOffsetKey(key);
        sizing.putLong(nextOffset);
        byte[] payload = new byte[sizing.position];
        Writer output = new Writer(payload);
        output.writeOffsetKey(key);
        output.putLong(nextOffset);
        return payload;
    }

    /**
     * Decodes an offset key and next offset from an OffsetStore record value.
     *
     * @param payload encoded offset-entry value
     * @return the decoded key and next offset
     * @throws CourseException if the payload is null, malformed, or contains trailing bytes
     */
    public static OffsetEntry decodeOffsetEntry(byte[] payload) {
        Reader input = new Reader(payload);
        try {
            OffsetEntry entry = new OffsetEntry(input.readOffsetKey(), input.readLong());
            input.requireEnd();
            return entry;
        } catch (CourseException exception) {
            throw exception;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw invalid("invalid offset entry", exception);
        }
    }

    /** Encodes one versioned application event as a compact record value. */
    public static byte[] encodeBusinessEvent(BusinessEvent event) {
        if (event == null) throw invalid("business event is null");
        Writer sizing = new Writer(null);
        sizing.putByte(1);
        sizing.putIdentifier(event.eventId(), "event id");
        sizing.putIdentifier(event.account(), "account");
        sizing.putLong(event.delta());
        byte[] payload = new byte[sizing.position];
        Writer output = new Writer(payload);
        output.putByte(1);
        output.putIdentifier(event.eventId(), "event id");
        output.putIdentifier(event.account(), "account");
        output.putLong(event.delta());
        return payload;
    }

    /** Decodes one versioned application event and rejects malformed or trailing bytes. */
    public static BusinessEvent decodeBusinessEvent(byte[] payload) {
        Reader input = new Reader(payload);
        try {
            if (input.readUnsignedByte() != 1) throw invalid("unknown business event version");
            BusinessEvent event =
                    new BusinessEvent(
                            input.readIdentifier("event id"),
                            input.readIdentifier("account"),
                            input.readLong());
            input.requireEnd();
            return event;
        } catch (CourseException exception) {
            throw exception;
        } catch (IllegalArgumentException | NullPointerException exception) {
            throw invalid("invalid business event", exception);
        }
    }

    private static void writeRequest(Writer output, Messages.Request request) {
        if (request instanceof Messages.MetadataRequest value) {
            output.putTopic(value.topic());
        } else if (request instanceof Messages.ProduceRequest value) {
            requireUnstampedRecords(value.records());
            output.writeTopicPartition(value.tp());
            output.putInt(value.epoch());
            if (value.acks() == null) throw invalid("acks is null");
            output.putShort(value.acks().wireId());
            output.putLong(value.timeoutMillis());
            output.writeRecordDataList(value.records());
        } else if (request instanceof Messages.IdempotentProduceRequest value) {
            validateIdempotentProducerFields(
                    value.timeoutMillis(),
                    value.producerId(),
                    value.producerEpoch(),
                    value.firstSequence(),
                    value.records());
            output.writeTopicPartition(value.tp());
            output.putInt(value.epoch());
            if (value.acks() == null) throw invalid("acks is null");
            output.putShort(value.acks().wireId());
            output.putLong(value.timeoutMillis());
            output.putLong(value.producerId());
            output.putInt(value.producerEpoch());
            output.putLong(value.firstSequence());
            output.writeRecordDataList(value.records());
        } else if (request instanceof Messages.FetchRequest value) {
            output.writeTopicPartition(value.tp());
            output.putInt(value.epoch());
            output.putLong(value.offset());
            output.putInt(value.maxRecords());
            output.putInt(value.maxBytes());
            output.writeOptionalToken(value.token());
        } else if (request instanceof Messages.CommitOffsetRequest value) {
            output.writeOffsetKey(value.key());
            output.putLong(value.nextOffset());
            output.writeOptionalToken(value.token());
        } else if (request instanceof Messages.FetchOffsetRequest value) {
            output.writeOffsetKey(value.key());
        } else if (request instanceof Messages.JoinGroupRequest value) {
            output.putIdentifier(value.group(), "group");
            output.putIdentifier(value.member(), "member");
            output.putTopic(value.topic());
        } else if (request instanceof Messages.HeartbeatRequest value) {
            output.writeOptionalToken(value.token());
        } else if (request instanceof Messages.LeaveGroupRequest value) {
            output.putIdentifier(value.group(), "group");
            output.putIdentifier(value.member(), "member");
        } else if (request instanceof Messages.GroupAssignmentRequest value) {
            output.putIdentifier(value.group(), "group");
            output.putIdentifier(value.member(), "member");
        } else if (request instanceof Messages.ReplicaFetchRequest value) {
            output.writeTopicPartition(value.tp());
            output.putInt(value.brokerId());
            output.putInt(value.epoch());
            output.putLong(value.fetchOffset());
            output.putInt(value.maxRecords());
            output.putInt(value.maxBytes());
            output.putBoolean(value.recoveryRead());
        } else {
            throw invalid("unsupported request type: " + request.getClass().getName());
        }
    }

    private static void writeReply(Writer output, Messages.Response response) {
        if (response instanceof Messages.MetadataBody value) {
            output.writeMetadataList(value.partitions());
        } else if (response instanceof Messages.ProduceBody value) {
            output.writeAppendResult(value.result());
        } else if (response instanceof Messages.FetchBody value) {
            output.writeLogRecordList(value.records());
            output.putLong(value.logStartOffset());
            output.putLong(value.logEndOffset());
            output.putLong(value.highWatermark());
            output.putInt(value.epoch());
        } else if (response instanceof Messages.EmptyBody) {
            // An empty response has an empty body.
        } else if (response instanceof Messages.OffsetBody value) {
            output.writeOptionalLong(value.nextOffset());
        } else if (response instanceof Messages.GroupBody value) {
            output.writeGroupAssignment(value.assignment());
        } else if (response instanceof Messages.ReplicaFetchBody value) {
            output.writeLogRecordList(value.records());
            output.putInt(value.epoch());
            output.putLong(value.highWatermark());
            output.putLong(value.leaderLogEndOffset());
        } else if (response instanceof Messages.ErrorBody value) {
            output.putString(
                    value.message(), MAX_PAYLOAD_LENGTH - Integer.BYTES, true, "error message");
        } else {
            throw invalid("unsupported response type: " + response.getClass().getName());
        }
    }

    private static Messages.ProduceRequest readProduceRequest(Reader input) {
        TopicPartition tp = input.readTopicPartition();
        int epoch = input.readInt();
        Acks acks = Acks.fromWireId(input.readShort());
        long timeoutMillis = input.readLong();
        List<RecordData> records = input.readRecordDataList();
        requireUnstampedRecords(records);
        return new Messages.ProduceRequest(tp, epoch, acks, timeoutMillis, records);
    }

    private static Messages.IdempotentProduceRequest readIdempotentProduceRequest(Reader input) {
        TopicPartition tp = input.readTopicPartition();
        int epoch = input.readInt();
        Acks acks = Acks.fromWireId(input.readShort());
        long timeoutMillis = input.readLong();
        long producerId = input.readLong();
        int producerEpoch = input.readInt();
        long firstSequence = input.readLong();
        List<RecordData> records = input.readRecordDataList();
        validateIdempotentProducerFields(
                timeoutMillis, producerId, producerEpoch, firstSequence, records);
        return new Messages.IdempotentProduceRequest(
                tp, epoch, acks, timeoutMillis, producerId, producerEpoch, firstSequence, records);
    }

    private static Messages.FetchRequest readFetchRequest(Reader input) {
        return new Messages.FetchRequest(
                input.readTopicPartition(),
                input.readInt(),
                input.readLong(),
                input.readInt(),
                input.readInt(),
                input.readOptionalToken());
    }

    private static Messages.CommitOffsetRequest readCommitOffsetRequest(Reader input) {
        return new Messages.CommitOffsetRequest(
                input.readOffsetKey(), input.readLong(), input.readOptionalToken());
    }

    private static Messages.ReplicaFetchRequest readReplicaFetchRequest(Reader input) {
        return new Messages.ReplicaFetchRequest(
                input.readTopicPartition(),
                input.readInt(),
                input.readInt(),
                input.readLong(),
                input.readInt(),
                input.readInt(),
                input.readBoolean());
    }

    private static Messages.FetchBody readFetchBody(Reader input) {
        return new Messages.FetchBody(
                input.readLogRecordList(),
                input.readLong(),
                input.readLong(),
                input.readLong(),
                input.readInt());
    }

    private static Messages.ReplicaFetchBody readReplicaFetchBody(Reader input) {
        return new Messages.ReplicaFetchBody(
                input.readLogRecordList(), input.readInt(), input.readLong(), input.readLong());
    }

    private static void validateIdempotentProducerFields(
            long timeoutMillis,
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records) {
        if (timeoutMillis < 0
                || producerId < 0
                || producerEpoch < 0
                || firstSequence < 0
                || records == null
                || records.isEmpty()) throw invalid("invalid idempotent producer request");
        try {
            Math.addExact(firstSequence, records.size());
        } catch (ArithmeticException exception) {
            throw invalid("producer sequence range overflows", exception);
        }
        requireUnstampedRecords(records);
        for (RecordData record : records) {
            long recordLength =
                    RECORD_FIXED_LENGTH
                            + ProducerStamp.ENCODED_OVERHEAD
                            + (record.key() == null ? 0L : record.key().length)
                            + record.value().length;
            if (recordLength > MAX_RECORD_LENGTH)
                throw invalid("idempotent producer record exceeds maximum length");
        }
    }

    private static void requireUnstampedRecords(List<RecordData> records) {
        if (records == null) throw invalid("record list is null");
        for (RecordData record : records) {
            if (record == null || record.producerStamp() != null)
                throw invalid("producer request records must be unstamped");
        }
    }

    private static void requireValidTopic(String topic) {
        if (topic == null || topic.isEmpty() || topic.equals(".") || topic.equals(".."))
            throw invalid("invalid topic identifier");
        for (int i = 0; i < topic.length(); i++) {
            char value = topic.charAt(i);
            if (!((value >= 'A' && value <= 'Z')
                    || (value >= 'a' && value <= 'z')
                    || (value >= '0' && value <= '9')
                    || value == '.'
                    || value == '_'
                    || value == '-')) throw invalid("invalid topic identifier");
        }
    }

    private static CourseException invalid(String message) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message);
    }

    private static CourseException invalid(String message, Throwable cause) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message, cause);
    }

    private static final class Writer {
        private final byte[] bytes;
        private int position;

        private Writer(byte[] bytes) {
            this.bytes = bytes;
        }

        private int reserve(int length) {
            if (length < 0 || (long) position + length > MAX_PAYLOAD_LENGTH)
                throw invalid("message body exceeds frame limit");
            int start = position;
            position += length;
            return start;
        }

        private void putBoolean(boolean value) {
            putByte(value ? 1 : 0);
        }

        private void putByte(int value) {
            int start = reserve(1);
            if (bytes != null) bytes[start] = (byte) value;
        }

        private void putShort(short value) {
            int start = reserve(Short.BYTES);
            if (bytes != null) {
                bytes[start] = (byte) (value >>> 8);
                bytes[start + 1] = (byte) value;
            }
        }

        private void putInt(int value) {
            int start = reserve(Integer.BYTES);
            if (bytes != null) {
                bytes[start] = (byte) (value >>> 24);
                bytes[start + 1] = (byte) (value >>> 16);
                bytes[start + 2] = (byte) (value >>> 8);
                bytes[start + 3] = (byte) value;
            }
        }

        private void putLong(long value) {
            int start = reserve(Long.BYTES);
            if (bytes != null) {
                bytes[start] = (byte) (value >>> 56);
                bytes[start + 1] = (byte) (value >>> 48);
                bytes[start + 2] = (byte) (value >>> 40);
                bytes[start + 3] = (byte) (value >>> 32);
                bytes[start + 4] = (byte) (value >>> 24);
                bytes[start + 5] = (byte) (value >>> 16);
                bytes[start + 6] = (byte) (value >>> 8);
                bytes[start + 7] = (byte) value;
            }
        }

        private void putString(String value, int maxBytes, boolean allowEmpty, String field) {
            if (value == null) throw invalid(field + " is null");
            int length = utf8Length(value, maxBytes, allowEmpty, field);
            putInt(length);
            int start = reserve(length);
            if (bytes != null) writeUtf8(bytes, start, value);
        }

        private void putIdentifier(String value, String field) {
            putString(value, MAX_IDENTIFIER_BYTES, false, field);
        }

        private void putTopic(String topic) {
            requireValidTopic(topic);
            putIdentifier(topic, "topic");
        }

        private void putBytes(byte[] value, boolean nullable, String field) {
            if (value == null) {
                if (!nullable) throw invalid(field + " is null");
                putInt(-1);
                return;
            }
            putInt(value.length);
            int start = reserve(value.length);
            if (bytes != null && value.length != 0)
                System.arraycopy(value, 0, bytes, start, value.length);
        }

        private void writeTopicPartition(TopicPartition tp) {
            if (tp == null) throw invalid("topic partition is null");
            putTopic(tp.topic());
            putInt(tp.partition());
        }

        private void writeOffsetKey(OffsetKey key) {
            if (key == null) throw invalid("offset key is null");
            putIdentifier(key.group(), "group");
            writeTopicPartition(key.tp());
        }

        private void writeToken(GroupToken token) {
            putIdentifier(token.group(), "group");
            putIdentifier(token.member(), "member");
            putInt(token.generation());
        }

        private void writeOptionalToken(GroupToken token) {
            putBoolean(token != null);
            if (token != null) writeToken(token);
        }

        private void writeRecordData(RecordData data) {
            if (data == null || data.value() == null)
                throw invalid("record data is null or has a null value");
            ProducerStamp stamp = data.producerStamp();
            long keyLength = data.key() == null ? 0 : data.key().length;
            long recordLength =
                    RECORD_FIXED_LENGTH
                            + keyLength
                            + data.value().length
                            + (stamp == null ? 0 : ProducerStamp.ENCODED_OVERHEAD);
            if (recordLength > MAX_RECORD_LENGTH) throw invalid("record exceeds maximum length");
            if (data.timestamp() < 0) throw invalid("record timestamp is negative");
            if (stamp != null) {
                putInt(-2);
                putLong(stamp.producerId());
                putInt(stamp.producerEpoch());
                putLong(stamp.firstSequence());
                putInt(stamp.batchSize());
                putInt(stamp.batchIndex());
                int start = reserve(stamp.batchHash().length);
                if (bytes != null)
                    System.arraycopy(stamp.batchHash(), 0, bytes, start, stamp.batchHash().length);
            }
            putBytes(data.key(), true, "record key");
            putBytes(data.value(), false, "record value");
            putLong(data.timestamp());
        }

        private void writeRecordDataList(List<RecordData> records) {
            if (records == null) throw invalid("record list is null");
            putInt(records.size());
            for (RecordData record : records) writeRecordData(record);
        }

        private void writeLogRecordList(List<LogRecord> records) {
            if (records == null) throw invalid("log record list is null");
            putInt(records.size());
            for (LogRecord record : records) {
                if (record == null) throw invalid("log record is null");
                putLong(record.offset());
                writeRecordData(record.data());
            }
        }

        private void writeIntList(List<Integer> values, String field) {
            if (values == null) throw invalid(field + " list is null");
            putInt(values.size());
            for (Integer value : values) {
                if (value == null) throw invalid(field + " contains null");
                putInt(value);
            }
        }

        private void writeMetadataList(List<PartitionMetadata> partitions) {
            if (partitions == null) throw invalid("metadata list is null");
            putInt(partitions.size());
            for (PartitionMetadata metadata : partitions) {
                if (metadata == null) throw invalid("metadata list contains null");
                writeTopicPartition(metadata.tp());
                putInt(metadata.leaderId());
                putInt(metadata.epoch());
                writeIntList(metadata.replicas(), "replica");
                writeIntList(metadata.isr(), "ISR");
                Endpoint endpoint = metadata.leader();
                if (endpoint == null) throw invalid("leader endpoint is null");
                putString(endpoint.host(), MAX_IDENTIFIER_BYTES, false, "endpoint host");
                putInt(endpoint.port());
            }
        }

        private void writeAppendResult(AppendResult result) {
            if (result == null) throw invalid("append result is null");
            putLong(result.firstOffset());
            putLong(result.nextOffset());
        }

        private void writeOptionalLong(java.util.OptionalLong value) {
            if (value == null) throw invalid("optional offset is null");
            putBoolean(value.isPresent());
            if (value.isPresent()) putLong(value.getAsLong());
        }

        private void writeGroupAssignment(GroupAssignment assignment) {
            if (assignment == null) throw invalid("group assignment is null");
            putInt(assignment.generation());
            TreeMap<String, List<TopicPartition>> sorted = new TreeMap<>(assignment.assignments());
            putInt(sorted.size());
            for (Map.Entry<String, List<TopicPartition>> entry : sorted.entrySet()) {
                putIdentifier(entry.getKey(), "member");
                List<TopicPartition> partitions = entry.getValue();
                if (partitions == null) throw invalid("assignment partition list is null");
                putInt(partitions.size());
                for (TopicPartition tp : partitions) writeTopicPartition(tp);
            }
        }
    }

    private static final class Reader {
        private final byte[] bytes;
        private int position;

        private Reader(byte[] bytes) {
            if (bytes == null) throw invalid("message body is null");
            if (bytes.length > MAX_PAYLOAD_LENGTH)
                throw invalid("message body exceeds frame limit");
            this.bytes = bytes;
        }

        private int remaining() {
            return bytes.length - position;
        }

        private void require(int length) {
            if (length < 0 || length > remaining())
                throw invalid("truncated or invalid message body");
        }

        private void requireEnd() {
            if (remaining() != 0) throw invalid("trailing bytes in message body");
        }

        private int readUnsignedByte() {
            require(1);
            return Byte.toUnsignedInt(bytes[position++]);
        }

        private boolean readBoolean() {
            int value = readUnsignedByte();
            if (value > 1) throw invalid("invalid boolean value");
            return value != 0;
        }

        private short readShort() {
            require(Short.BYTES);
            int value =
                    (Byte.toUnsignedInt(bytes[position]) << 8)
                            | Byte.toUnsignedInt(bytes[position + 1]);
            position += Short.BYTES;
            return (short) value;
        }

        private int readInt() {
            require(Integer.BYTES);
            int value =
                    (Byte.toUnsignedInt(bytes[position]) << 24)
                            | (Byte.toUnsignedInt(bytes[position + 1]) << 16)
                            | (Byte.toUnsignedInt(bytes[position + 2]) << 8)
                            | Byte.toUnsignedInt(bytes[position + 3]);
            position += Integer.BYTES;
            return value;
        }

        private long readLong() {
            require(Long.BYTES);
            long value =
                    ((long) Byte.toUnsignedInt(bytes[position]) << 56)
                            | ((long) Byte.toUnsignedInt(bytes[position + 1]) << 48)
                            | ((long) Byte.toUnsignedInt(bytes[position + 2]) << 40)
                            | ((long) Byte.toUnsignedInt(bytes[position + 3]) << 32)
                            | ((long) Byte.toUnsignedInt(bytes[position + 4]) << 24)
                            | ((long) Byte.toUnsignedInt(bytes[position + 5]) << 16)
                            | ((long) Byte.toUnsignedInt(bytes[position + 6]) << 8)
                            | Byte.toUnsignedInt(bytes[position + 7]);
            position += Long.BYTES;
            return value;
        }

        private String readString(int maxBytes, boolean allowEmpty, String field) {
            int length = readInt();
            if (length < 0 || length > maxBytes || (!allowEmpty && length == 0))
                throw invalid("invalid " + field + " length");
            require(length);
            try {
                ByteBuffer input = ByteBuffer.wrap(bytes, position, length);
                CharBuffer decoded =
                        StandardCharsets.UTF_8
                                .newDecoder()
                                .onMalformedInput(CodingErrorAction.REPORT)
                                .onUnmappableCharacter(CodingErrorAction.REPORT)
                                .decode(input);
                position += length;
                return decoded.toString();
            } catch (CharacterCodingException exception) {
                throw invalid("invalid UTF-8 in " + field, exception);
            }
        }

        private String readIdentifier(String field) {
            return readString(MAX_IDENTIFIER_BYTES, false, field);
        }

        private String readTopic() {
            String topic = readIdentifier("topic");
            requireValidTopic(topic);
            return topic;
        }

        private byte[] readBytes(int length, String field) {
            require(length);
            byte[] value = new byte[length];
            if (length != 0) System.arraycopy(bytes, position, value, 0, length);
            position += length;
            return value;
        }

        private int readCount(int minimumElementBytes, String field) {
            int count = readInt();
            if (count < 0 || count > remaining() / minimumElementBytes)
                throw invalid("invalid " + field + " count");
            return count;
        }

        private TopicPartition readTopicPartition() {
            return new TopicPartition(readTopic(), readInt());
        }

        private OffsetKey readOffsetKey() {
            return new OffsetKey(readIdentifier("group"), readTopicPartition());
        }

        private GroupToken readToken() {
            return new GroupToken(readIdentifier("group"), readIdentifier("member"), readInt());
        }

        private GroupToken readOptionalToken() {
            return readBoolean() ? readToken() : null;
        }

        private RecordData readRecordData() {
            int keyLength = readInt();
            ProducerStamp stamp = null;
            if (keyLength == -2) {
                long producerId = readLong();
                int producerEpoch = readInt();
                long firstSequence = readLong();
                int batchSize = readInt();
                int batchIndex = readInt();
                byte[] batchHash = readBytes(32, "producer batch hash");
                try {
                    stamp =
                            new ProducerStamp(
                                    producerId,
                                    producerEpoch,
                                    firstSequence,
                                    batchSize,
                                    batchIndex,
                                    batchHash);
                } catch (IllegalArgumentException exception) {
                    throw invalid("invalid producer stamp", exception);
                }
                keyLength = readInt();
            }
            if (keyLength < -1) throw invalid("invalid record key length");
            long stampBytes = stamp == null ? 0 : ProducerStamp.ENCODED_OVERHEAD;
            long keyBytes = keyLength < 0 ? 0L : keyLength;
            if (RECORD_FIXED_LENGTH + stampBytes + keyBytes > MAX_RECORD_LENGTH)
                throw invalid("invalid record key length");
            byte[] key = keyLength == -1 ? null : readBytes(keyLength, "record key");
            int valueLength = readInt();
            long recordLength = RECORD_FIXED_LENGTH + stampBytes + keyBytes + valueLength;
            if (valueLength < 0 || recordLength > MAX_RECORD_LENGTH)
                throw invalid("invalid record value length");
            byte[] value = readBytes(valueLength, "record value");
            long timestamp = readLong();
            return new RecordData(key, value, timestamp, stamp);
        }

        private List<RecordData> readRecordDataList() {
            int count = readCount(16, "record");
            List<RecordData> records = new ArrayList<>(count);
            for (int i = 0; i < count; i++) records.add(readRecordData());
            return records;
        }

        private List<LogRecord> readLogRecordList() {
            int count = readCount(24, "log record");
            List<LogRecord> records = new ArrayList<>(count);
            for (int i = 0; i < count; i++)
                records.add(new LogRecord(readLong(), readRecordData()));
            return records;
        }

        private List<Integer> readIntList(String field) {
            int count = readCount(Integer.BYTES, field);
            List<Integer> values = new ArrayList<>(count);
            for (int i = 0; i < count; i++) values.add(readInt());
            return values;
        }

        private List<PartitionMetadata> readMetadataList() {
            int count = readCount(34, "metadata");
            List<PartitionMetadata> partitions = new ArrayList<>(count);
            for (int i = 0; i < count; i++) {
                TopicPartition tp = readTopicPartition();
                int leaderId = readInt();
                int epoch = readInt();
                List<Integer> replicas = readIntList("replica");
                List<Integer> isr = readIntList("ISR");
                Endpoint leader =
                        new Endpoint(
                                readString(MAX_IDENTIFIER_BYTES, false, "endpoint host"),
                                readInt());
                partitions.add(new PartitionMetadata(tp, leaderId, epoch, replicas, isr, leader));
            }
            return partitions;
        }

        private AppendResult readAppendResult() {
            return new AppendResult(readLong(), readLong());
        }

        private java.util.OptionalLong readOptionalLong() {
            return readBoolean()
                    ? java.util.OptionalLong.of(readLong())
                    : java.util.OptionalLong.empty();
        }

        private GroupAssignment readGroupAssignment() {
            int generation = readInt();
            int count = readCount(9, "group member");
            TreeMap<String, List<TopicPartition>> assignments = new TreeMap<>();
            for (int i = 0; i < count; i++) {
                String member = readIdentifier("member");
                int partitionCount = readCount(9, "assignment partition");
                List<TopicPartition> partitions = new ArrayList<>(partitionCount);
                for (int p = 0; p < partitionCount; p++) partitions.add(readTopicPartition());
                if (assignments.putIfAbsent(member, partitions) != null)
                    throw invalid("duplicate group member in assignment");
            }
            return new GroupAssignment(generation, assignments);
        }
    }

    private static int utf8Length(String value, int maxBytes, boolean allowEmpty, String field) {
        long length = 0;
        for (int i = 0; i < value.length(); i++) {
            char current = value.charAt(i);
            if (current <= 0x7f) {
                length++;
            } else if (current <= 0x7ff) {
                length += 2;
            } else if (Character.isHighSurrogate(current)) {
                if (i + 1 >= value.length() || !Character.isLowSurrogate(value.charAt(i + 1)))
                    throw invalid("invalid UTF-16 in " + field);
                i++;
                length += 4;
            } else if (Character.isLowSurrogate(current)) {
                throw invalid("invalid UTF-16 in " + field);
            } else {
                length += 3;
            }
            if (length > maxBytes) throw invalid(field + " exceeds byte limit");
        }
        if (!allowEmpty && length == 0) throw invalid(field + " is empty");
        return (int) length;
    }

    private static void writeUtf8(byte[] target, int position, String value) {
        int index = position;
        for (int i = 0; i < value.length(); i++) {
            int codePoint = value.charAt(i);
            if (codePoint <= 0x7f) {
                target[index++] = (byte) codePoint;
            } else if (codePoint <= 0x7ff) {
                target[index++] = (byte) (0xc0 | (codePoint >>> 6));
                target[index++] = (byte) (0x80 | (codePoint & 0x3f));
            } else if (Character.isHighSurrogate((char) codePoint)) {
                int low = value.charAt(++i);
                codePoint = Character.toCodePoint((char) codePoint, (char) low);
                target[index++] = (byte) (0xf0 | (codePoint >>> 18));
                target[index++] = (byte) (0x80 | ((codePoint >>> 12) & 0x3f));
                target[index++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3f));
                target[index++] = (byte) (0x80 | (codePoint & 0x3f));
            } else {
                target[index++] = (byte) (0xe0 | (codePoint >>> 12));
                target[index++] = (byte) (0x80 | ((codePoint >>> 6) & 0x3f));
                target[index++] = (byte) (0x80 | (codePoint & 0x3f));
            }
        }
    }
}
