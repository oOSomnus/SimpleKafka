package io.simplekafka.group;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.OffsetKey;
import io.simplekafka.storage.PartitionLog;

import java.nio.file.Path;
import java.util.Objects;
import java.util.OptionalLong;

/** Durable append-only next-offset store backed by the course partition log. */
public final class OffsetStore implements AutoCloseable {
    private static final long SEGMENT_BYTES = 1_048_576;
    private final PartitionLog log;
    private boolean closed;

    /**
     * Opens the backing partition log with 1 MiB segments and an index interval of one record.
     *
     * @param file path used as the backing partition-log directory
     * @throws NullPointerException if {@code file} is null
     * @throws io.simplekafka.CourseException if the backing log cannot be initialized
     * @throws ExerciseNotImplementedException if opening existing segments reaches the unfinished
     *     Step 4 recovery method
     */
    public OffsetStore(Path file) {
        this.log = new PartitionLog(Objects.requireNonNull(file, "file"), SEGMENT_BYTES, 1);
    }

    /**
     * Step 15: append the group's next offset to the durable store using
     * {@code MessageCodec.encodeOffsetEntry}, a null record key, and timestamp zero. Commits may
     * move backward; the latest appended value is the current value. Test: Step15Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 15.
     *
     * @param key group-and-partition offset key
     * @param nextOffset next offset to persist
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for a null key or negative offset, or
     *     with {@link io.simplekafka.ErrorCode#STORAGE_ERROR} if the store is closed or storage
     *     fails
     * @throws ExerciseNotImplementedException while the Step 15 exercise method is a skeleton
     */
    public void commit(OffsetKey key, long nextOffset) {
        throw new ExerciseNotImplementedException(15, "commit");
    }

    /**
     * Step 15: return the last committed next offset for this group/partition pair, or
     * {@link OptionalLong#empty()} if none is present. The latest appended value remains current
     * after reopening the store. Test: Step15Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 15.
     *
     * @param key group-and-partition offset key
     * @return the stored next offset, or an empty value when no commit exists
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST} for a null key, or with
     *     {@link io.simplekafka.ErrorCode#STORAGE_ERROR} if the store is closed or storage fails
     * @throws ExerciseNotImplementedException while the Step 15 exercise method is a skeleton
     */
    public OptionalLong fetch(OffsetKey key) {
        throw new ExerciseNotImplementedException(15, "fetch");
    }

    /**
     * Closes the backing log; repeated calls have no effect.
     *
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#STORAGE_ERROR} if closing the backing log fails
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        log.close();
    }
}
