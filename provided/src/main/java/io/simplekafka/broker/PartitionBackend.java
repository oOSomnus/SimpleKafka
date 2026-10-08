package io.simplekafka.broker;

import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;

import java.util.List;

/**
 * Operations on a single partition used by broker request handlers.
 *
 * <p>Ordinary fetches expose only the committed prefix, ending at the exclusive high watermark.
 * Replica fetches use {@link io.simplekafka.replication.ReplicaFetchBackend} and may read beyond
 * that boundary.
 */
public interface PartitionBackend {
    /**
     * Appends a batch and returns its assigned offset range after the requested acknowledgement.
     *
     * <p>{@code Acks.LEADER} requires the local append; {@code Acks.ALL} waits until the high
     * watermark covers the appended range.
     *
     * @param records non-empty batch to append
     * @param acks acknowledgement mode; {@code Acks.LEADER} waits for the local append, while
     *     {@code Acks.ALL} waits for the high watermark to cover the batch
     * @param epoch leader epoch against which this operation is fenced
     * @param timeoutMillis maximum wait for the required acknowledgement; a timeout does not mean
     *     the append was not applied
     * @return the appended batch's half-open offset range
     * @throws io.simplekafka.CourseException if arguments or records are invalid ({@link
     *     io.simplekafka.ErrorCode#INVALID_REQUEST}), the epoch is stale ({@link
     *     io.simplekafka.ErrorCode#FENCED_EPOCH}), this broker is not the online leader ({@link
     *     io.simplekafka.ErrorCode#NOT_LEADER}), the ISR is below minISR for {@code Acks.ALL}
     *     ({@link io.simplekafka.ErrorCode#NOT_ENOUGH_REPLICAS}), the required acknowledgement
     *     times out ({@link io.simplekafka.ErrorCode#REQUEST_TIMEOUT}; the append may already have
     *     succeeded), or storage fails / the log is corrupt ({@link
     *     io.simplekafka.ErrorCode#STORAGE_ERROR} or {@link
     *     io.simplekafka.ErrorCode#CORRUPT_RECORD})
     */
    AppendResult produce(List<RecordData> records, Acks acks, int epoch, long timeoutMillis);

    /**
     * Fetches records beginning at {@code offset}, subject to both limits.
     *
     * <p>Only records below the high watermark are returned. An offset at or above the high
     * watermark but no greater than the log end therefore produces an empty result, even when an
     * uncommitted suffix exists.
     *
     * @param offset first requested offset, within the inclusive range from log start to log end
     * @param maxRecords maximum number of records; must be positive
     * @param maxBytes maximum byte budget, measured using complete on-disk records including each
     *     four-byte length prefix; must be positive
     * @param epoch leader epoch against which this operation is fenced
     * @return the available committed records in offset order
     * @throws io.simplekafka.CourseException if arguments are invalid ({@link
     *     io.simplekafka.ErrorCode#INVALID_REQUEST}), the epoch is stale ({@link
     *     io.simplekafka.ErrorCode#FENCED_EPOCH}), this broker is not the online leader ({@link
     *     io.simplekafka.ErrorCode#NOT_LEADER}), the offset is outside the retained log range
     *     ({@link io.simplekafka.ErrorCode#OFFSET_OUT_OF_RANGE}), or storage fails / the log is
     *     corrupt ({@link io.simplekafka.ErrorCode#STORAGE_ERROR} or {@link
     *     io.simplekafka.ErrorCode#CORRUPT_RECORD})
     */
    List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch);

    /**
     * Returns the first offset currently retained by this partition.
     *
     * @return the inclusive beginning of the retained offset range
     */
    long logStartOffset();

    /**
     * Returns the next offset that would be assigned by an append.
     *
     * @return the exclusive end of the log
     */
    long logEndOffset();

    /**
     * Returns the exclusive boundary of the prefix visible to ordinary fetches.
     *
     * @return the high watermark; records with smaller offsets are committed
     */
    long highWatermark();

    /**
     * Returns the current leader epoch used to fence partition operations.
     *
     * @return the current leader epoch
     */
    int epoch();
}
