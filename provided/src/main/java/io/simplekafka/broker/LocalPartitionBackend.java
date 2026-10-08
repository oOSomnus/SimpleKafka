package io.simplekafka.broker;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;

import java.util.List;
import java.util.Objects;

public final class LocalPartitionBackend implements PartitionBackend {
    private final PartitionLog log;

    public LocalPartitionBackend(PartitionLog log) {
        this.log = Objects.requireNonNull(log);
    }

    @Override
    public AppendResult produce(
            List<RecordData> records, Acks acks, int epoch, long timeoutMillis) {
        if (epoch != 0)
            throw new CourseException(ErrorCode.FENCED_EPOCH, "single broker epoch is 0");
        if (timeoutMillis < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "negative timeout");
        return log.append(records);
    }

    @Override
    public List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch) {
        if (epoch != 0)
            throw new CourseException(ErrorCode.FENCED_EPOCH, "single broker epoch is 0");
        return log.read(offset, maxRecords, maxBytes);
    }

    @Override
    public long logStartOffset() {
        return log.logStartOffset();
    }

    @Override
    public long logEndOffset() {
        return log.logEndOffset();
    }

    @Override
    public long highWatermark() {
        return log.logEndOffset();
    }

    @Override
    public int epoch() {
        return 0;
    }
}
