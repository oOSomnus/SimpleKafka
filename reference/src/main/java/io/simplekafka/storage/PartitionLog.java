package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.IndexEntry;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.Stream;

/** Partition-wide collection of contiguous, individually indexed log segments. */
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

    public synchronized AppendResult append(List<RecordData> records) {
        if (records == null || records.isEmpty())
            throw new CourseException(ErrorCode.INVALID_REQUEST, "append batch must not be empty");
        lock.lock();
        try {
            ensureOpen();
            long firstOffset = logEndOffsetLocked();
            int[] recordSizes = new int[records.size()];
            long candidateNext = firstOffset;
            for (int i = 0; i < records.size(); i++) {
                recordSizes[i] = RecordCodec.encodedSize(records.get(i));
                try {
                    candidateNext = Math.addExact(candidateNext, 1L);
                } catch (ArithmeticException exception) {
                    throw new CourseException(
                            ErrorCode.INVALID_REQUEST, "log end offset overflow", exception);
                }
            }
            markMutation();

            long nextOffset = firstOffset;
            int index = 0;
            while (index < records.size()) {
                SegmentEntry active = segments.lastEntry().getValue();
                long currentBytes = active.log.sizeBytes();
                if (currentBytes > 0 && recordSizes[index] > segmentBytes - currentBytes) {
                    active = createSegment(nextOffset);
                    currentBytes = 0;
                }
                long startPosition = currentBytes;
                long plannedBytes = currentBytes;
                int end = index;
                while (end < records.size()) {
                    int recordBytes = recordSizes[end];
                    if (end > index
                            && (plannedBytes >= segmentBytes
                                    || recordBytes > segmentBytes - plannedBytes)) break;
                    if (end == index
                            && plannedBytes > 0
                            && recordBytes > segmentBytes - plannedBytes) break;
                    plannedBytes += recordBytes;
                    end++;
                    if (plannedBytes > segmentBytes) break;
                }
                if (end == index) {
                    active = createSegment(nextOffset);
                    continue;
                }

                AppendResult segmentResult = active.log.append(records.subList(index, end));
                if (segmentResult.firstOffset() != nextOffset)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "segment append returned a discontinuous offset");
                long bytePosition = startPosition;
                for (int recordIndex = index; recordIndex < end; recordIndex++) {
                    active.index.add(nextOffset, bytePosition);
                    bytePosition += recordSizes[recordIndex];
                    nextOffset++;
                }
                if (segmentResult.nextOffset() != nextOffset)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "segment append returned an invalid next offset");
                index = end;
            }
            if (nextOffset != candidateNext)
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD,
                        "partition append returned an invalid next offset");
            return new AppendResult(firstOffset, nextOffset);
        } finally {
            lock.unlock();
        }
    }

    public List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
        if (maxRecords <= 0 || maxBytes <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "read limits must be positive");
        lock.lock();
        try {
            ensureOpen();
            long startOffset = logStartOffsetLocked();
            long endOffset = logEndOffsetLocked();
            if (offset < startOffset || offset > endOffset)
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE,
                        "offset is outside the retained partition log");
            if (offset == endOffset) return List.of();

            ArrayList<LogRecord> records = new ArrayList<>(Math.min(maxRecords, 16));
            long nextOffset = offset;
            long bytesRead = 0;
            for (SegmentEntry entry : segments.values()) {
                long segmentStart = entry.log.baseOffset();
                long segmentEnd = entry.log.logEndOffset();
                if (nextOffset >= segmentEnd) continue;
                if (nextOffset < segmentStart)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD, "partition segments contain an offset gap");
                int remainingRecords = maxRecords - records.size();
                int remainingBytes = (int) Math.min(Integer.MAX_VALUE, maxBytes - bytesRead);
                if (remainingRecords <= 0 || remainingBytes <= 0) break;
                IndexEntry hint = entry.index.floor(nextOffset);
                List<LogRecord> part =
                        entry.log.readFrom(
                                nextOffset, hint.position(), remainingRecords, remainingBytes);
                if (part.isEmpty()) {
                    if (nextOffset < segmentEnd) break;
                    continue;
                }
                for (LogRecord record : part) {
                    records.add(record);
                    bytesRead += RecordCodec.encodedSize(record.data());
                    nextOffset = record.offset() + 1;
                }
                if (records.size() >= maxRecords
                        || bytesRead >= maxBytes
                        || nextOffset < segmentEnd) break;
            }
            return List.copyOf(records);
        } finally {
            lock.unlock();
        }
    }

    public synchronized long deleteBefore(long offset) {
        if (offset < 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "retention offset must be nonnegative");
        lock.lock();
        try {
            ensureOpen();
            boolean mutationMarked = false;
            while (segments.size() > 1) {
                Map.Entry<Long, SegmentEntry> first = segments.firstEntry();
                SegmentEntry entry = first.getValue();
                if (entry.log.logEndOffset() > offset) break;
                if (!mutationMarked) {
                    markMutation();
                    mutationMarked = true;
                }
                closeEntry(entry);
                deleteEntryFiles(entry);
                segments.remove(first.getKey());
            }
            return logStartOffsetLocked();
        } finally {
            lock.unlock();
        }
    }

    public synchronized void truncateTo(long nextOffset) {
        lock.lock();
        try {
            ensureOpen();
            long startOffset = logStartOffsetLocked();
            long endOffset = logEndOffsetLocked();
            if (nextOffset < startOffset || nextOffset > endOffset)
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE,
                        "truncate offset is outside the retained partition log");
            if (nextOffset == endOffset) return;
            Map.Entry<Long, SegmentEntry> targetEntry = segments.floorEntry(nextOffset);
            if (targetEntry == null)
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "no segment contains the truncate offset");
            SegmentEntry target = targetEntry.getValue();
            if (nextOffset > target.log.logEndOffset())
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "partition segments contain an offset gap");
            markMutation();

            target.log.truncateToOffset(nextOffset);
            target.index.rebuild(target.log.path(), target.log.baseOffset());
            List<Map.Entry<Long, SegmentEntry>> removed =
                    new ArrayList<>(segments.tailMap(nextOffset, false).entrySet());
            for (Map.Entry<Long, SegmentEntry> entry : removed) {
                closeEntry(entry.getValue());
                deleteEntryFiles(entry.getValue());
                segments.remove(entry.getKey());
            }
        } finally {
            lock.unlock();
        }
    }

    public long logStartOffset() {
        lock.lock();
        try {
            ensureOpen();
            return logStartOffsetLocked();
        } finally {
            lock.unlock();
        }
    }

    public long logEndOffset() {
        lock.lock();
        try {
            ensureOpen();
            return logEndOffsetLocked();
        } finally {
            lock.unlock();
        }
    }

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

    private long logStartOffsetLocked() {
        return segments.firstKey();
    }

    private long logEndOffsetLocked() {
        return segments.lastEntry().getValue().log.logEndOffset();
    }

    private void closeEntry(SegmentEntry entry) {
        RuntimeException failure = null;
        try {
            entry.log.close();
        } catch (RuntimeException exception) {
            failure = exception;
        }
        try {
            entry.index.close();
        } catch (RuntimeException exception) {
            if (failure == null) failure = exception;
            else failure.addSuppressed(exception);
        }
        if (failure != null) throw failure;
    }

    private void deleteEntryFiles(SegmentEntry entry) {
        try {
            Files.deleteIfExists(entry.log.path());
            Files.deleteIfExists(entry.index.path());
        } catch (IOException exception) {
            throw storageError("delete retired segment " + entry.log.path(), exception);
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
