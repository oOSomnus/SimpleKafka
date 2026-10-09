package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;
import io.simplekafka.protocol.ProducerBatchFingerprint;
import io.simplekafka.storage.PartitionLog;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Derives producer sequence state from the retained partition log and appends stamped batches. */
public final class IdempotentAppender {
    private static final int MAX_RECORD_BYTES = 1_048_580;

    private final PartitionLog log;
    private final Map<Long, ProducerState> producers = new HashMap<>();
    private boolean cacheValid;
    private long cachedPrefixVersion;
    private long scannedLeo;
    private PartialBatch partial;

    public IdempotentAppender(PartitionLog log) {
        if (log == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "partition log is null");
        this.log = log;
    }

    /** Appends a batch once, or returns the durable range for an exact retry. */
    public AppendResult append(
            long producerId, int producerEpoch, long firstSequence, List<RecordData> records) {
        synchronized (log) {
            byte[] batchHash = validateRequest(producerId, producerEpoch, firstSequence, records);
            long sequenceEnd = sequenceEnd(firstSequence, records.size());
            try {
                ensureCache();
                ProducerState state = producers.get(producerId);
                if (state != null && producerEpoch < state.epoch)
                    throw failure(
                            ErrorCode.FENCED_PRODUCER_EPOCH,
                            "producer epoch is older than the durable epoch");

                if (partial != null) {
                    return continuePartial(
                            producerId,
                            producerEpoch,
                            firstSequence,
                            records,
                            batchHash,
                            sequenceEnd);
                }

                if (state != null && producerEpoch == state.epoch) {
                    CompletedBatch known = state.completed.get(firstSequence);
                    if (known != null) {
                        if (known.batchSize != records.size())
                            throw failure(
                                    ErrorCode.OUT_OF_ORDER_SEQUENCE,
                                    "retry batch boundary differs from the durable batch");
                        if (!Arrays.equals(known.batchHash, batchHash))
                            throw failure(
                                    ErrorCode.DUPLICATE_SEQUENCE_CONFLICT,
                                    "retry batch payload differs from the durable batch");
                        return known.range;
                    }
                }
                validateNextBatch(state, producerEpoch, firstSequence);
                List<RecordData> stamped =
                        stampBatch(producerId, producerEpoch, firstSequence, records, batchHash, 0);
                AppendResult range = log.append(stamped);
                ProducerState updated = state;
                if (updated == null || producerEpoch > updated.epoch)
                    updated = new ProducerState(producerEpoch);
                updated.completed.put(
                        firstSequence, new CompletedBatch(range, records.size(), batchHash));
                updated.nextSequence = sequenceEnd;
                producers.put(producerId, updated);
                scannedLeo = range.nextOffset();
                cachedPrefixVersion = log.prefixVersion();
                cacheValid = true;
                return range;
            } catch (RuntimeException exception) {
                if (isUncertainStorageFailure(exception)) invalidateCache();
                throw exception;
            }
        }
    }

    /** Discards all derived state and reconstructs it from the retained disk prefix. */
    public void recover() {
        synchronized (log) {
            try {
                long start = log.logStartOffset();
                if (start != 0) {
                    invalidateCache();
                    throw failure(
                            ErrorCode.PRODUCER_STATE_EXPIRED,
                            "producer state is unavailable after log retention");
                }
                rebuild(log.prefixVersion(), log.logEndOffset());
            } catch (RuntimeException exception) {
                invalidateCache();
                throw exception;
            }
        }
    }

    private byte[] validateRequest(
            long producerId, int producerEpoch, long firstSequence, List<RecordData> records) {
        if (producerId < 0 || producerEpoch < 0 || firstSequence < 0)
            throw failure(ErrorCode.INVALID_REQUEST, "producer identity or sequence is negative");
        if (records == null || records.isEmpty())
            throw failure(ErrorCode.INVALID_REQUEST, "producer batch must not be empty");
        for (RecordData record : records) {
            if (record == null || record.value() == null || record.producerStamp() != null)
                throw failure(
                        ErrorCode.INVALID_REQUEST,
                        "producer batch records must be valid and unstamped");
            long size =
                    96L + (record.key() == null ? 0L : record.key().length) + record.value().length;
            if (size > MAX_RECORD_BYTES)
                throw failure(ErrorCode.INVALID_REQUEST, "producer record exceeds the 1 MiB limit");
        }
        sequenceEnd(firstSequence, records.size());
        return ProducerBatchFingerprint.fingerprint(records);
    }

    private static long sequenceEnd(long firstSequence, int count) {
        try {
            return Math.addExact(firstSequence, count);
        } catch (ArithmeticException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "producer sequence range overflows", exception);
        }
    }

    private static long checkedOffsetEnd(long firstOffset, int count) {
        try {
            return Math.addExact(firstOffset, count);
        } catch (ArithmeticException exception) {
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "producer batch offset range overflows", exception);
        }
    }

    private void ensureCache() {
        long start = log.logStartOffset();
        if (start != 0) {
            invalidateCache();
            throw failure(
                    ErrorCode.PRODUCER_STATE_EXPIRED,
                    "producer state is unavailable after log retention");
        }
        long prefixVersion = log.prefixVersion();
        long leo = log.logEndOffset();
        if (!cacheValid || prefixVersion != cachedPrefixVersion || leo < scannedLeo) {
            rebuild(prefixVersion, leo);
        } else if (leo > scannedLeo) {
            try {
                scan(scannedLeo, leo);
                scannedLeo = leo;
            } catch (RuntimeException exception) {
                invalidateCache();
                throw exception;
            }
        }
    }

    private void rebuild(long prefixVersion, long leo) {
        invalidateCache();
        try {
            scan(0, leo);
            scannedLeo = leo;
            cachedPrefixVersion = prefixVersion;
            cacheValid = true;
        } catch (RuntimeException exception) {
            invalidateCache();
            throw exception;
        }
    }

    private void scan(long fromOffset, long toOffset) {
        long offset = fromOffset;
        while (offset < toOffset) {
            List<LogRecord> page = log.read(offset, 1, MAX_RECORD_BYTES);
            if (page.size() != 1 || page.getFirst().offset() != offset)
                throw failure(ErrorCode.CORRUPT_RECORD, "producer-state replay made no progress");
            replayRecord(page.getFirst());
            offset++;
        }
    }

    private void replayRecord(LogRecord record) {
        ProducerStamp stamp = record.data().producerStamp();
        if (stamp == null) {
            if (partial != null)
                throw failure(
                        ErrorCode.CORRUPT_RECORD,
                        "an incomplete producer batch is followed by another record");
            return;
        }
        if (partial == null) {
            if (stamp.batchIndex() != 0)
                throw failure(
                        ErrorCode.CORRUPT_RECORD, "producer batch does not start at index zero");
            beginReplayBatch(stamp, record.offset());
        } else {
            if (!sameBatch(partial.header, stamp) || stamp.batchIndex() != partial.storedCount)
                throw failure(ErrorCode.CORRUPT_RECORD, "producer batch metadata is discontinuous");
        }
        partial.fingerprint.update(record.data());
        partial.storedCount++;
        if (partial.storedCount == stamp.batchSize()) {
            byte[] actualHash = partial.fingerprint.finish();
            if (!Arrays.equals(actualHash, partial.header.batchHash()))
                throw failure(
                        ErrorCode.CORRUPT_RECORD, "producer batch fingerprint does not match");
            ProducerState state = producers.get(stamp.producerId());
            long nextSequence = sequenceEnd(stamp.firstSequence(), stamp.batchSize());
            AppendResult range =
                    new AppendResult(
                            partial.firstOffset,
                            checkedOffsetEnd(partial.firstOffset, stamp.batchSize()));
            state.completed.put(
                    stamp.firstSequence(),
                    new CompletedBatch(range, stamp.batchSize(), partial.header.batchHash()));
            state.nextSequence = nextSequence;
            partial = null;
        }
    }

    private void beginReplayBatch(ProducerStamp stamp, long offset) {
        ProducerState state = producers.get(stamp.producerId());
        if (state == null) {
            if (stamp.firstSequence() != 0)
                throw failure(
                        ErrorCode.CORRUPT_RECORD,
                        "unknown producer sequence does not start at zero");
            state = new ProducerState(stamp.producerEpoch());
            producers.put(stamp.producerId(), state);
        } else if (stamp.producerEpoch() < state.epoch) {
            throw failure(ErrorCode.CORRUPT_RECORD, "producer epoch moves backwards in the log");
        } else if (stamp.producerEpoch() > state.epoch) {
            if (stamp.firstSequence() != 0)
                throw failure(
                        ErrorCode.CORRUPT_RECORD, "new producer epoch does not start at zero");
            state = new ProducerState(stamp.producerEpoch());
            producers.put(stamp.producerId(), state);
        } else if (stamp.firstSequence() != state.nextSequence) {
            throw failure(
                    ErrorCode.CORRUPT_RECORD, "producer sequence is discontinuous in the log");
        }
        partial = new PartialBatch(stamp, offset);
    }

    private AppendResult continuePartial(
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records,
            byte[] batchHash,
            long sequenceEnd) {
        ProducerStamp header = partial.header;
        if (header.producerId() != producerId
                || header.producerEpoch() != producerEpoch
                || header.firstSequence() != firstSequence)
            throw failure(
                    ErrorCode.INCOMPLETE_BATCH,
                    "the incomplete producer batch must be completed before another batch");
        if (header.batchSize() != records.size())
            throw failure(
                    ErrorCode.OUT_OF_ORDER_SEQUENCE,
                    "retry batch boundary differs from the incomplete batch");
        if (!Arrays.equals(header.batchHash(), batchHash))
            throw failure(
                    ErrorCode.DUPLICATE_SEQUENCE_CONFLICT,
                    "retry batch payload differs from the incomplete batch");
        verifyPartialPrefix(records);
        List<RecordData> suffix =
                stampBatch(
                        producerId,
                        producerEpoch,
                        firstSequence,
                        records,
                        batchHash,
                        partial.storedCount);
        long firstOffset = partial.firstOffset;
        long expectedNext = checkedOffsetEnd(firstOffset, records.size());
        long expectedFirst = checkedOffsetEnd(firstOffset, partial.storedCount);
        AppendResult appended = log.append(suffix);
        if (appended.firstOffset() != expectedFirst || appended.nextOffset() != expectedNext)
            throw failure(
                    ErrorCode.CORRUPT_RECORD, "completed producer batch has an invalid range");
        ProducerState state = producers.get(producerId);
        state.completed.put(
                firstSequence,
                new CompletedBatch(
                        new AppendResult(firstOffset, expectedNext), records.size(), batchHash));
        state.nextSequence = sequenceEnd;
        partial = null;
        scannedLeo = expectedNext;
        cachedPrefixVersion = log.prefixVersion();
        cacheValid = true;
        return new AppendResult(firstOffset, expectedNext);
    }

    private void verifyPartialPrefix(List<RecordData> requested) {
        for (int index = 0; index < partial.storedCount; index++) {
            long offset = checkedOffsetEnd(partial.firstOffset, index);
            List<LogRecord> page = log.read(offset, 1, MAX_RECORD_BYTES);
            if (page.size() != 1 || page.getFirst().offset() != offset)
                throw failure(
                        ErrorCode.CORRUPT_RECORD, "incomplete producer batch prefix is missing");
            LogRecord actual = page.getFirst();
            ProducerStamp stamp = actual.data().producerStamp();
            if (stamp == null
                    || !sameBatch(partial.header, stamp)
                    || stamp.batchIndex() != index
                    || !sameContent(actual.data(), requested.get(index)))
                throw failure(
                        ErrorCode.CORRUPT_RECORD,
                        "incomplete producer batch prefix is inconsistent");
        }
    }

    private static List<RecordData> stampBatch(
            long producerId,
            int producerEpoch,
            long firstSequence,
            List<RecordData> records,
            byte[] batchHash,
            int fromIndex) {
        ArrayList<RecordData> stamped = new ArrayList<>(records.size() - fromIndex);
        for (int index = fromIndex; index < records.size(); index++) {
            RecordData data = records.get(index);
            ProducerStamp stamp =
                    new ProducerStamp(
                            producerId,
                            producerEpoch,
                            firstSequence,
                            records.size(),
                            index,
                            batchHash);
            stamped.add(new RecordData(data.key(), data.value(), data.timestamp(), stamp));
        }
        return stamped;
    }

    private static void validateNextBatch(
            ProducerState state, int producerEpoch, long firstSequence) {
        if (state == null) {
            if (firstSequence != 0)
                throw failure(
                        ErrorCode.OUT_OF_ORDER_SEQUENCE,
                        "new producer must start at sequence zero");
            return;
        }
        if (producerEpoch < state.epoch)
            throw failure(
                    ErrorCode.FENCED_PRODUCER_EPOCH,
                    "producer epoch is older than the durable epoch");
        if (producerEpoch > state.epoch) {
            if (firstSequence != 0)
                throw failure(
                        ErrorCode.OUT_OF_ORDER_SEQUENCE,
                        "new producer epoch must start at sequence zero");
            return;
        }
        if (firstSequence != state.nextSequence)
            throw failure(
                    ErrorCode.OUT_OF_ORDER_SEQUENCE,
                    "producer sequence is not the next expected sequence");
    }

    private static boolean sameBatch(ProducerStamp left, ProducerStamp right) {
        return left.producerId() == right.producerId()
                && left.producerEpoch() == right.producerEpoch()
                && left.firstSequence() == right.firstSequence()
                && left.batchSize() == right.batchSize()
                && Arrays.equals(left.batchHash(), right.batchHash());
    }

    private static boolean sameContent(RecordData left, RecordData right) {
        return left.timestamp() == right.timestamp()
                && Arrays.equals(left.key(), right.key())
                && Arrays.equals(left.value(), right.value());
    }

    private static boolean isUncertainStorageFailure(RuntimeException exception) {
        return exception instanceof CourseException courseException
                && (courseException.code() == ErrorCode.STORAGE_ERROR
                        || courseException.code() == ErrorCode.CORRUPT_RECORD);
    }

    private void invalidateCache() {
        producers.clear();
        partial = null;
        cacheValid = false;
        scannedLeo = 0;
        cachedPrefixVersion = 0;
    }

    private static CourseException failure(ErrorCode code, String message) {
        return new CourseException(code, message);
    }

    private static final class ProducerState {
        private final int epoch;
        private final Map<Long, CompletedBatch> completed = new HashMap<>();
        private long nextSequence;

        private ProducerState(int epoch) {
            this.epoch = epoch;
        }
    }

    private record CompletedBatch(AppendResult range, int batchSize, byte[] batchHash) {}

    private static final class PartialBatch {
        private final ProducerStamp header;
        private final long firstOffset;
        private final ProducerBatchFingerprint fingerprint;
        private int storedCount;

        private PartialBatch(ProducerStamp header, long firstOffset) {
            this.header = header;
            this.firstOffset = firstOffset;
            this.fingerprint = new ProducerBatchFingerprint(header.batchSize());
        }
    }
}
