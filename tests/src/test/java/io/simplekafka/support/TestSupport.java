package io.simplekafka.support;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import org.junit.jupiter.api.function.Executable;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertThrows;

/** Domain fixtures and assertions shared by the permanent behavior tests. */
public final class TestSupport {
    private TestSupport() {}

    public static CourseException assertCode(ErrorCode expected, Executable action) {
        CourseException exception = assertThrows(CourseException.class, action);
        assertEquals(expected, exception.code());
        return exception;
    }

    public static RecordData record(String key, String value, long timestamp) {
        return new RecordData(key == null ? null : utf8(key), utf8(value), timestamp);
    }

    public static RecordData value(String value) {
        return record(null, value, 0);
    }

    public static byte[] utf8(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }

    public static List<RecordData> values(String prefix, int count) {
        List<RecordData> records = new ArrayList<>(count);
        for (int index = 0; index < count; index++) records.add(value(prefix + index));
        return List.copyOf(records);
    }

    public static AppendResult append(RpcClient client, TopicPartition tp, List<RecordData> records) {
        Messages.Reply reply = client.call(new Messages.ProduceRequest(tp, 0, Acks.LEADER, 5_000, records));
        assertEquals(ErrorCode.NONE, reply.error(), "produce reply: " + reply.body());
        return assertInstanceOf(Messages.ProduceBody.class, reply.body()).result();
    }

    public static Messages.FetchBody fetch(RpcClient client, TopicPartition tp, long offset,
                                           int maxRecords, int maxBytes) {
        Messages.Reply reply = client.call(new Messages.FetchRequest(tp, 0, offset, maxRecords, maxBytes, null));
        assertEquals(ErrorCode.NONE, reply.error(), "fetch reply: " + reply.body());
        return assertInstanceOf(Messages.FetchBody.class, reply.body());
    }

    public static List<String> values(List<LogRecord> records) {
        return records.stream().map(record -> new String(record.data().value(), StandardCharsets.UTF_8)).toList();
    }
}
