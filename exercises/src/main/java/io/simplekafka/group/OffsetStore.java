package io.simplekafka.group;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.model.RecordData;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.storage.PartitionLog;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.OptionalLong;
import java.util.TreeMap;

/** Durable append-only next-offset store backed by the course partition log. */
public final class OffsetStore implements AutoCloseable {
    private static final long SEGMENT_BYTES = 1_048_576;
    private final PartitionLog log;
    private final Map<OffsetKey, Long> committed = new TreeMap<>();
    private boolean closed;

    /**
     * Opens the backing partition log with 1 MiB segments and index interval one, then replays its
     * committed offset records.
     *
     * @param file path used as the backing partition-log directory
     * @throws NullPointerException if {@code file} is null
     * @throws CourseException with {@link ErrorCode#CORRUPT_RECORD} for corrupt log or replayed
     *     offset records, or with {@link ErrorCode#STORAGE_ERROR} if storage initialization fails
     * @throws ExerciseNotImplementedException if existing-segment recovery, index rebuilding, or
     *     replaying a nonempty log reaches the unfinished Step 4, Step 5, or Step 6 methods
     */
    public OffsetStore(Path file) {
        this.log = new PartitionLog(Objects.requireNonNull(file, "file"), SEGMENT_BYTES, 1);
        try {
            loadCommittedOffsets();
        } catch (RuntimeException failure) {
            try {
                log.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /**
     * Step 15: append the group's next offset to the durable store using {@code
     * MessageCodec.encodeOffsetEntry}, a null record key, and timestamp zero. Commits may move
     * backward; the latest appended value is the current value. Test: Step15Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 15.
     *
     * @param key group-and-partition offset key
     * @param nextOffset next offset to persist
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     *     for a null key or negative offset, or with {@link io.simplekafka.ErrorCode#STORAGE_ERROR}
     *     if the store is closed or storage fails
     * @throws ExerciseNotImplementedException while the Step 15 exercise method is a skeleton
     */
    public synchronized void commit(OffsetKey key, long nextOffset) {
        throw new ExerciseNotImplementedException(15, "commit");
    }

    /**
     * Step 15: return the last committed next offset for this group/partition pair, or {@link
     * OptionalLong#empty()} if none is present. The latest appended value remains current after
     * reopening the store. Test: Step15Test. Lesson: docs/book/chapters/04-client-offset.tex, Step
     * 15.
     *
     * @param key group-and-partition offset key
     * @return the stored next offset, or an empty value when no commit exists
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     *     for a null key, or with {@link io.simplekafka.ErrorCode#STORAGE_ERROR} if the store is
     *     closed or storage fails
     * @throws ExerciseNotImplementedException while the Step 15 exercise method is a skeleton
     */
    public synchronized OptionalLong fetch(OffsetKey key) {
        throw new ExerciseNotImplementedException(15, "fetch");
    }

    private void loadCommittedOffsets() {
        long nextOffset = log.logStartOffset();
        long logEndOffset = log.logEndOffset();
        while (nextOffset < logEndOffset) {
            List<LogRecord> records = log.read(nextOffset, 1_024, (int) SEGMENT_BYTES);
            if (records.isEmpty())
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "offset-store replay made no progress");
            for (LogRecord record : records) {
                RecordData data = record.data();
                if (data.key() != null || data.timestamp() != 0) {
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "invalid offset-store record at " + record.offset());
                }
                try {
                    MessageCodec.OffsetEntry entry = MessageCodec.decodeOffsetEntry(data.value());
                    committed.put(entry.key(), entry.nextOffset());
                } catch (RuntimeException exception) {
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "invalid offset-store value at " + record.offset(),
                            exception);
                }
            }
            nextOffset = Math.addExact(records.get(records.size() - 1).offset(), 1);
        }
    }

    /**
     * Closes the backing log; repeated calls have no effect.
     *
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#STORAGE_ERROR} if
     *     closing the backing log fails
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        committed.clear();
        log.close();
    }
}
