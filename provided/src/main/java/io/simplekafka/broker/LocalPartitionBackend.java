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

/** A single-broker partition backend that exposes all locally appended records as committed. */
public final class LocalPartitionBackend implements PartitionBackend {
    private final PartitionLog log;

    /**
     * Creates a backend that delegates partition storage operations to {@code log}.
     *
     * @param log partition log used by this backend
     * @throws NullPointerException if {@code log} is {@code null}
     */
    public LocalPartitionBackend(PartitionLog log) {
        this.log = Objects.requireNonNull(log);
    }

    /**
     * Appends records to the local log without waiting for replication acknowledgements.
     *
     * @param records non-empty batch to append
     * @param acks acknowledgement mode; ignored by this single-broker backend
     * @param epoch expected epoch, which must be {@code 0}
     * @param timeoutMillis timeout value; negative values are rejected, while nonnegative values
     *     are not used to wait
     * @return the offset range returned by the log append
     * @throws CourseException if {@code epoch} is not {@code 0} ({@link ErrorCode#FENCED_EPOCH}),
     *     {@code timeoutMillis} is negative ({@link ErrorCode#INVALID_REQUEST}), the batch is
     *     invalid ({@link ErrorCode#INVALID_REQUEST} or {@link ErrorCode#CORRUPT_RECORD}), or
     *     storage fails ({@link ErrorCode#STORAGE_ERROR})
     */
    @Override
    public AppendResult produce(
            List<RecordData> records, Acks acks, int epoch, long timeoutMillis) {
        if (epoch != 0)
            throw new CourseException(ErrorCode.FENCED_EPOCH, "single broker epoch is 0");
        if (timeoutMillis < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "negative timeout");
        return log.append(records);
    }

    /**
     * Reads locally retained records at or after {@code offset}; this backend's high watermark
     * equals its log end, so every available record is in the committed prefix.
     *
     * @param offset first requested offset in the inclusive range from log start to log end
     * @param maxRecords maximum number of records; must be positive
     * @param maxBytes maximum byte budget, measured using complete on-disk records including each
     *     four-byte length prefix; must be positive
     * @param epoch expected epoch, which must be {@code 0}
     * @return the records returned by the partition log
     * @throws CourseException if {@code epoch} is not {@code 0} ({@link ErrorCode#FENCED_EPOCH}),
     *     arguments are invalid ({@link ErrorCode#INVALID_REQUEST}), {@code offset} is outside the
     *     retained log range ({@link ErrorCode#OFFSET_OUT_OF_RANGE}), or storage fails / the log is
     *     corrupt ({@link ErrorCode#STORAGE_ERROR} or {@link ErrorCode#CORRUPT_RECORD})
     */
    @Override
    public List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch) {
        if (epoch != 0)
            throw new CourseException(ErrorCode.FENCED_EPOCH, "single broker epoch is 0");
        return log.read(offset, maxRecords, maxBytes);
    }

    /**
     * Returns the first offset currently retained by the partition log.
     *
     * @return the inclusive beginning of the retained offset range
     * @throws CourseException if the underlying log cannot be read ({@link
     *     ErrorCode#STORAGE_ERROR})
     */
    @Override
    public long logStartOffset() {
        return log.logStartOffset();
    }

    /**
     * Returns the next offset that would be assigned by an append.
     *
     * @return the exclusive end of the local log
     * @throws CourseException if the underlying log cannot be read ({@link
     *     ErrorCode#STORAGE_ERROR})
     */
    @Override
    public long logEndOffset() {
        return log.logEndOffset();
    }

    /**
     * Returns the local log end as its high watermark; this backend has no replication lag.
     *
     * @return the exclusive boundary of the committed prefix, equal to {@link #logEndOffset()}
     * @throws CourseException if the underlying log cannot be read ({@link
     *     ErrorCode#STORAGE_ERROR})
     */
    @Override
    public long highWatermark() {
        return log.logEndOffset();
    }

    /**
     * Returns the fixed epoch used by a single-broker partition.
     *
     * @return the fixed leader epoch, {@code 0}
     */
    @Override
    public int epoch() {
        return 0;
    }
}
