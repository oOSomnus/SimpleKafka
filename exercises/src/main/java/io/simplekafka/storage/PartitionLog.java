package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/** Partition-wide collection of log segments and step 6–8 algorithms. */
public final class PartitionLog implements AutoCloseable {
    private final Path directory;
    private final long segmentBytes;
    private final int indexInterval;
    private final ReentrantLock lock = new ReentrantLock();
    private final TreeMap<Long, SegmentEntry> segments = new TreeMap<>();
    private boolean closed;
    private long mutationVersion;
    private ProducerStamp tailProducerStamp;
    private long prefixVersion;
    private boolean failed;

    /**
     * Creates the directory, opens its numbered segments, recovers each log, and rebuilds each
     * sparse index. An empty directory starts with a segment at offset zero; initialization failure
     * closes resources already opened.
     *
     * @param directory directory containing the partition's segment and index files
     * @param segmentBytes target maximum byte size for a segment
     * @param indexInterval number of records between sparse-index entries
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for invalid configuration,
     *     with {@link ErrorCode#CORRUPT_RECORD} for corrupt or discontinuous segments, or with
     *     {@link ErrorCode#STORAGE_ERROR} if storage initialization fails
     * @throws ExerciseNotImplementedException if opening existing segments reaches the unfinished
     *     Step 4 recovery method
     */
    public PartitionLog(Path directory, long segmentBytes, int indexInterval) {
        if (directory == null || segmentBytes <= 0 || indexInterval <= 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid partition log configuration");
        this.directory = directory;
        this.segmentBytes = segmentBytes;
        this.indexInterval = indexInterval;
        initialize();
    }

    /**
     * Step 6: append a prevalidated batch across byte-limited segments with contiguous global
     * offsets. A segment may exceed its target size to hold one record. Call {@link
     * #markMutation()} once after all input validation and before the first write. See Step06Test
     * and book step 6.
     *
     * @param records non-empty records to append in order
     * @return the appended half-open global offset range
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an empty or invalid batch
     *     or offset overflow, or with {@link ErrorCode#STORAGE_ERROR} for storage failure or a
     *     closed log
     * @throws ExerciseNotImplementedException while the Step 6 exercise method is a skeleton
     */
    public synchronized AppendResult append(List<RecordData> records) {
        throw new ExerciseNotImplementedException(6, "PartitionLog.append");
    }

    /**
     * Step 6: read across segments using the sparse floor entry for the requested offset as a hint
     * while respecting total record and byte limits. Offset equal to LEO returns an empty immutable
     * list. See Step06Test and book step 6.
     *
     * @param offset first global offset to read
     * @param maxRecords maximum total records to return across segments
     * @param maxBytes maximum total encoded bytes to return across segments
     * @return the records beginning at {@code offset}, bounded by both limits
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for nonpositive limits, with
     *     {@link ErrorCode#OFFSET_OUT_OF_RANGE} for an offset outside the retained range, with
     *     {@link ErrorCode#CORRUPT_RECORD} for a segment gap or invalid record, or with {@link
     *     ErrorCode#STORAGE_ERROR} for storage failure or a closed log
     * @throws ExerciseNotImplementedException while the Step 6 exercise method is a skeleton
     */
    public List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(6, "PartitionLog.read");
    }

    /**
     * Step 7: delete closed segments whose end offset is at or below the requested bound, while
     * keeping at least one segment, and return the actual new start. Call {@link #markMutation()}
     * once before the first deletion that changes the log. See Step07Test and book step 7.
     *
     * @param offset requested lower bound for retained data
     * @return the actual start offset after any eligible segments are deleted
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if {@code offset} is negative,
     *     or with {@link ErrorCode#STORAGE_ERROR} for storage failure or a closed log
     * @throws ExerciseNotImplementedException while the Step 7 exercise method is a skeleton
     */
    public synchronized long deleteBefore(long offset) {
        throw new ExerciseNotImplementedException(7, "PartitionLog.deleteBefore");
    }

    /**
     * Step 8: retain the prefix below the requested next offset, remove later segments, and rebuild
     * the active index. This is a no-op at the current LEO. For a valid non-no-op truncation, call
     * {@link #markMutation()} before the first write/delete. See Step08Test and book step 8.
     *
     * @param nextOffset boundary to retain within {@code [logStartOffset(), logEndOffset()]}
     * @throws CourseException with {@link ErrorCode#OFFSET_OUT_OF_RANGE} if the boundary is outside
     *     the retained range, or with {@link ErrorCode#STORAGE_ERROR} for storage failure or a
     *     closed log
     * @throws ExerciseNotImplementedException while the Step 8 exercise method is a skeleton
     */
    public synchronized void truncateTo(long nextOffset) {
        throw new ExerciseNotImplementedException(8, "PartitionLog.truncateTo");
    }

    /**
     * Returns the base offset of the first retained segment.
     *
     * @return the first retained global offset
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if this log is closed
     */
    public long logStartOffset() {
        lock.lock();
        try {
            ensureOpen();
            return logStartOffsetLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the next global offset after the last record in the final segment.
     *
     * @return the log end offset
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if this log is closed
     */
    public long logEndOffset() {
        lock.lock();
        try {
            ensureOpen();
            return logEndOffsetLocked();
        } finally {
            lock.unlock();
        }
    }

    /**
     * In-memory generation for detecting mutations made through this open log instance; recovery
     * proofs use it to detect later local changes.
     *
     * @return the mutation version
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if this log is closed
     */
    public long mutationVersion() {
        lock.lock();
        try {
            ensureOpen();
            return mutationVersion;
        } finally {
            lock.unlock();
        }
    }

    public ProducerStamp tailProducerStamp() {
        lock.lock();
        try {
            ensureOpen();
            return tailProducerStamp;
        } finally {
            lock.unlock();
        }
    }

    public long prefixVersion() {
        lock.lock();
        try {
            ensureOpen();
            return prefixVersion;
        } finally {
            lock.unlock();
        }
    }

    private void initialize() {
        try {
            Files.createDirectories(directory);
            List<Path> logFiles;
            try (Stream<Path> children = Files.list(directory)) {
                logFiles =
                        children.filter(Files::isRegularFile)
                                .filter(
                                        path ->
                                                path.getFileName()
                                                        .toString()
                                                        .matches("[0-9]{20}\\.log"))
                                .sorted()
                                .toList();
            }
            if (logFiles.isEmpty()) {
                createSegment(0);
                return;
            }
            long expectedBase = -1;
            for (Path logFile : logFiles) {
                String name = logFile.getFileName().toString();
                long baseOffset;
                try {
                    baseOffset = Long.parseLong(name.substring(0, 20));
                } catch (NumberFormatException exception) {
                    throw new CourseException(
                            ErrorCode.STORAGE_ERROR,
                            "invalid segment filename: " + name,
                            exception);
                }
                SegmentLog log = SegmentLog.open(logFile, baseOffset);
                SparseIndex index = null;
                try {
                    index = new SparseIndex(indexPath(logFile), baseOffset, indexInterval);
                    index.rebuild(logFile, baseOffset);
                    if (expectedBase >= 0 && baseOffset != expectedBase)
                        throw new CourseException(
                                ErrorCode.CORRUPT_RECORD,
                                "partition segments are not offset-contiguous");
                    if (segments.putIfAbsent(baseOffset, new SegmentEntry(log, index)) != null)
                        throw new CourseException(
                                ErrorCode.STORAGE_ERROR,
                                "duplicate segment base offset " + baseOffset);
                    expectedBase = log.logEndOffset();
                } catch (RuntimeException exception) {
                    try {
                        log.close();
                    } catch (RuntimeException closeFailure) {
                        exception.addSuppressed(closeFailure);
                    }
                    if (index != null) {
                        try {
                            index.close();
                        } catch (RuntimeException closeFailure) {
                            exception.addSuppressed(closeFailure);
                        }
                    }
                    throw exception;
                }
            }
            if (segments.isEmpty()) createSegment(0);
            refreshTailProducerStamp();
        } catch (IOException exception) {
            closeAfterInitializationFailure(exception);
            throw storageError("initialize partition log " + directory, exception);
        } catch (RuntimeException exception) {
            closeAfterInitializationFailure(exception);
            throw exception;
        }
    }

    private void refreshTailProducerStamp() {
        tailProducerStamp = null;
        Map.Entry<Long, SegmentEntry> tail = segments.lastEntry();
        while (tail != null) {
            SegmentLog log = tail.getValue().log;
            if (log.logEndOffset() > log.baseOffset()) {
                tailProducerStamp = log.tailProducerStamp();
                return;
            }
            tail = segments.lowerEntry(tail.getKey());
        }
    }

    private SegmentEntry createSegment(long baseOffset) {
        Path logPath = directory.resolve(String.format(Locale.ROOT, "%020d.log", baseOffset));
        SegmentLog log = SegmentLog.create(logPath, baseOffset);
        SparseIndex index = null;
        try {
            index = new SparseIndex(indexPath(logPath), baseOffset, indexInterval);
            index.reset();
            SegmentEntry entry = new SegmentEntry(log, index);
            segments.put(baseOffset, entry);
            return entry;
        } catch (RuntimeException exception) {
            try {
                log.close();
            } catch (RuntimeException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            if (index != null) {
                try {
                    index.close();
                } catch (RuntimeException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
            }
            throw exception;
        }
    }

    private long logStartOffsetLocked() {
        return segments.firstKey();
    }

    private long logEndOffsetLocked() {
        return segments.lastEntry().getValue().log.logEndOffset();
    }

    private void closeAfterInitializationFailure(Throwable failure) {
        for (SegmentEntry entry : segments.values()) {
            try {
                entry.log.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            try {
                entry.index.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
        }
        segments.clear();
    }

    private void ensureOpen() {
        if (closed)
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR, "partition log is closed: " + directory);
        if (failed)
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR,
                    "partition log is failed and must be reopened: " + directory);
    }

    private static boolean poisonsAfterMutation(RuntimeException exception) {
        return exception instanceof CourseException courseException
                && (courseException.code() == ErrorCode.STORAGE_ERROR
                        || courseException.code() == ErrorCode.CORRUPT_RECORD);
    }

    /**
     * Call under the partition-log lock immediately before the first persistent mutation in a valid
     * operation.
     */
    private void markMutation() {
        mutationVersion++;
    }

    private static Path indexPath(Path logFile) {
        String name = logFile.getFileName().toString();
        return logFile.resolveSibling(
                name.substring(0, name.length() - ".log".length()) + ".index");
    }

    private static CourseException storageError(String action, IOException cause) {
        return new CourseException(ErrorCode.STORAGE_ERROR, "failed to " + action, cause);
    }

    /**
     * Closes all segments and indexes; repeated calls have no effect. Open-state checks on this log
     * report {@link ErrorCode#STORAGE_ERROR} after closure.
     *
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if closing a segment or index
     *     fails; additional close failures are suppressed on the first failure
     */
    @Override
    public synchronized void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            RuntimeException failure = null;
            for (SegmentEntry entry : segments.values()) {
                try {
                    entry.log.close();
                } catch (RuntimeException exception) {
                    if (failure == null) failure = exception;
                    else failure.addSuppressed(exception);
                }
                try {
                    entry.index.close();
                } catch (RuntimeException exception) {
                    if (failure == null) failure = exception;
                    else failure.addSuppressed(exception);
                }
            }
            if (failure != null) throw failure;
        } finally {
            lock.unlock();
        }
    }

    private static final class SegmentEntry {
        private final SegmentLog log;
        private final SparseIndex index;

        private SegmentEntry(SegmentLog log, SparseIndex index) {
            this.log = log;
            this.index = index;
        }
    }
}
