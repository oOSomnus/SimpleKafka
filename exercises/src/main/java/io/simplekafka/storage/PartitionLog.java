package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Locale;
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
     * offsets. Call {@link #markMutation()} once after all input validation and before the first
     * write. See Step06Test and book step 6.
     */
    public synchronized AppendResult append(List<RecordData> records) {
        throw new ExerciseNotImplementedException(6, "PartitionLog.append");
    }

    /**
     * Step 6: read across segments from an indexed hint while respecting total record and byte
     * limits. See Step06Test and book step 6.
     */
    public List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(6, "PartitionLog.read");
    }

    /**
     * Step 7: delete only closed segments whose final offset is before the requested bound; return
     * the actual new start. Call {@link #markMutation()} once before the first deletion that
     * changes the log. See Step07Test and book step 7.
     */
    public synchronized long deleteBefore(long offset) {
        throw new ExerciseNotImplementedException(7, "PartitionLog.deleteBefore");
    }

    /**
     * Step 8: retain the prefix below the requested next offset, remove later segments, and rebuild
     * the active index. For a valid non-no-op truncation, call {@link #markMutation()} before the
     * first write/delete. See Step08Test and book step 8.
     */
    public synchronized void truncateTo(long nextOffset) {
        throw new ExerciseNotImplementedException(8, "PartitionLog.truncateTo");
    }

    public long logStartOffset() {
        lock.lock();
        try {
            ensureOpen();
            return segments.firstKey();
        } finally {
            lock.unlock();
        }
    }

    public long logEndOffset() {
        lock.lock();
        try {
            ensureOpen();
            return segments.lastEntry().getValue().log.logEndOffset();
        } finally {
            lock.unlock();
        }
    }

    /** In-memory generation for detecting changes made through this open log instance. */
    public long mutationVersion() {
        lock.lock();
        try {
            ensureOpen();
            return mutationVersion;
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
                    Path indexPath = indexPath(logFile);
                    index = new SparseIndex(indexPath, baseOffset, indexInterval);
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
        } catch (IOException exception) {
            closeAfterInitializationFailure(exception);
            throw storageError("initialize partition log " + directory, exception);
        } catch (RuntimeException exception) {
            closeAfterInitializationFailure(exception);
            throw exception;
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
