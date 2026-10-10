package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;
import java.util.stream.IntStream;

/** One append-only, offset-contiguous on-disk log segment. */
public final class SegmentLog implements AutoCloseable {
    private final Path path;
    private final FileChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    private final long baseOffset;
    private long nextOffset;
    private ProducerStamp tailProducerStamp;
    private boolean closed;

    private SegmentLog(Path path, FileChannel channel, long baseOffset) {
        this.path = path;
        this.channel = channel;
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
    }

    /**
     * Creates a new segment file and its parent directories.
     *
     * @param path path of the new segment file
     * @param baseOffset first offset represented by this segment
     * @return the open segment
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for a null path, negative
     *     base, or existing file, or with {@link ErrorCode#STORAGE_ERROR} if creation fails
     */
    public static SegmentLog create(Path path, long baseOffset) {
        validatePathAndBase(path, baseOffset);
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            FileChannel channel =
                    FileChannel.open(
                            path,
                            StandardOpenOption.CREATE_NEW,
                            StandardOpenOption.READ,
                            StandardOpenOption.WRITE);
            return new SegmentLog(path, channel, baseOffset);
        } catch (FileAlreadyExistsException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "segment already exists: " + path, exception);
        } catch (IOException exception) {
            throw storageError("create segment " + path, exception);
        }
    }

    /**
     * Opens a segment for reading and writing, then recovers its next offset from its contents. If
     * recovery fails, closes the segment before rethrowing the failure.
     *
     * @param path path of the existing segment file
     * @param baseOffset first offset represented by this segment
     * @return the recovered open segment
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for a null path or negative
     *     base, with {@link ErrorCode#CORRUPT_RECORD} for complete corrupt contents, or with {@link
     *     ErrorCode#STORAGE_ERROR} if opening or recovery I/O fails; an I/O cause is retained
     * @throws ExerciseNotImplementedException if recovery reaches the unfinished Step 4 method
     */
    public static SegmentLog open(Path path, long baseOffset) {
        validatePathAndBase(path, baseOffset);
        FileChannel channel;
        try {
            channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
        } catch (IOException exception) {
            throw storageError("open segment " + path, exception);
        }
        SegmentLog segment = new SegmentLog(path, channel, baseOffset);
        try {
            segment.recover();
            return segment;
        } catch (RuntimeException exception) {
            try {
                segment.close();
            } catch (RuntimeException closeFailure) {
                exception.addSuppressed(closeFailure);
            }
            throw exception;
        }
    }

    /**
     * Step 2: validate every record before writing, append a nonempty batch at consecutive offsets,
     * force the channel once, and return its half-open offset range. Invalid input or offset
     * overflow leaves the file unchanged. See Step02Test and book step 2.
     *
     * @param records non-empty records to append in order
     * @return the appended half-open offset range
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an invalid batch or offset
     *     overflow, or with {@link ErrorCode#STORAGE_ERROR} if the write fails or this segment is
     *     closed
     * @throws ExerciseNotImplementedException while the Step 2 exercise method is a skeleton
     */
    public AppendResult append(List<RecordData> records) {
        lock.lock();
        try {
            if (closed) {
                throw new CourseException(ErrorCode.STORAGE_ERROR, "segment closed");
            }
            if (records == null || records.isEmpty()) {
                throw new CourseException(ErrorCode.INVALID_REQUEST, "records must not be empty");
            }
            List<ByteBuffer> byteBuffers;
            try {
                byteBuffers =
                        new ArrayList<>(
                                IntStream.range(0, records.size())
                                        .mapToObj(
                                                i -> new LogRecord(i + nextOffset, records.get(i)))
                                        .map(RecordCodec::encode)
                                        .toList());
            } catch (IllegalArgumentException | NullPointerException exception) {
                throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid input");
            }
            long resultOffset = nextOffset;
            FileChannel channel;
            try {
                channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            } catch (IOException exception) {
                throw storageError("open segment " + path, exception);
            }
            for (ByteBuffer byteBuffer : byteBuffers) {
                try {
                    resultOffset = Math.addExact(resultOffset, 1);
                    byteBuffer.order(ByteOrder.BIG_ENDIAN);
                    ChannelIO.writeFully(channel, byteBuffer, channel.size());
                } catch (IOException exception) {
                    throw storageError("write segment " + path, exception);
                } catch (ArithmeticException exception) {
                    throw new CourseException(ErrorCode.INVALID_REQUEST, "segment overflow");
                }
            }
            AppendResult appendResult = new AppendResult(nextOffset, resultOffset);
            nextOffset = resultOffset;
            try {
                channel.force(false);
            } catch (IOException exception) {
                throw storageError("write segment " + path, exception);
            }
            return appendResult;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Reads from {@code offset} by delegating to {@link #readFrom(long, long, int, int)} with a
     * byte-position hint of zero, the segment-start position.
     *
     * @param offset first segment offset to read
     * @param maxRecords maximum records to return
     * @param maxBytes maximum encoded bytes to return
     * @return the records within both limits
     * @throws CourseException if the delegated read rejects the offset or limits, encounters
     *     corruption, or fails because the segment is closed or storage fails
     * @throws ExerciseNotImplementedException while the delegated Step 3 method is a skeleton
     */
    public List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
        return readFrom(offset, 0, maxRecords, maxBytes);
    }

    /**
     * Step 3: read complete records within both limits from a verified record-boundary hint without
     * changing append position. A hint cannot skip the requested offset; an EOF hint at LEO returns
     * an empty list. See Step03Test and book step 3.
     *
     * @param offset first segment offset to read
     * @param bytePositionHint file position at a verified record boundary
     * @param maxRecords maximum records to return
     * @param maxBytes maximum encoded bytes to return
     * @return the records within both limits
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for a negative, out-of-file,
     *     non-boundary, or skipping hint or a nonpositive limit; with {@link
     *     ErrorCode#OFFSET_OUT_OF_RANGE} when {@code offset} is outside this segment, with {@link
     *     ErrorCode#CORRUPT_RECORD} for corrupt stored contents, or with {@link
     *     ErrorCode#STORAGE_ERROR} for storage failure or a closed segment
     * @throws ExerciseNotImplementedException while the Step 3 exercise method is a skeleton
     */
    public List<LogRecord> readFrom(
            long offset, long bytePositionHint, int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(3, "SegmentLog.readFrom");
    }

    /**
     * Step 4: scan contiguous valid records, truncate only an incomplete tail, and return the
     * recovered LEO. Complete corruption is rejected without truncation. See Step04Test and book
     * step 4.
     *
     * @return the recovered next offset
     * @throws CourseException with {@link ErrorCode#CORRUPT_RECORD} for invalid complete contents
     *     or offset overflow, or with {@link ErrorCode#STORAGE_ERROR} if recovery I/O fails
     * @throws ExerciseNotImplementedException while the Step 4 exercise method is a skeleton
     */
    public long recover() {
        throw new ExerciseNotImplementedException(4, "SegmentLog.recover");
    }

    /**
     * Returns this segment's immutable base offset.
     *
     * @return the first offset represented by this segment
     */
    public long baseOffset() {
        return baseOffset;
    }

    /**
     * Returns the next offset after the last record in this segment.
     *
     * @return the segment log end offset
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if this segment is closed
     */
    public long logEndOffset() {
        lock.lock();
        try {
            ensureOpen();
            return nextOffset;
        } finally {
            lock.unlock();
        }
    }

    ProducerStamp tailProducerStamp() {
        lock.lock();
        try {
            ensureOpen();
            return tailProducerStamp;
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns the current segment file size in bytes.
     *
     * @return the file size
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if this segment is closed or
     *     reading its size fails
     */
    public long sizeBytes() {
        lock.lock();
        try {
            ensureOpen();
            return channel.size();
        } catch (IOException exception) {
            throw storageError("read segment size", exception);
        } finally {
            lock.unlock();
        }
    }

    /**
     * Returns this segment's file path.
     *
     * @return the segment file path
     */
    public Path path() {
        return path;
    }

    void truncateToOffset(long offset) {
        lock.lock();
        try {
            ensureOpen();
            if (offset < baseOffset || offset > nextOffset)
                throw new CourseException(
                        ErrorCode.OFFSET_OUT_OF_RANGE, "truncate offset is outside this segment");
            if (offset == nextOffset) return;
            long fileSize = channel.size();
            long position = 0;
            long expectedOffset = baseOffset;
            ProducerStamp retainedTail = null;
            while (position < fileSize) {
                if (expectedOffset == offset) break;
                RecordAt found = readRecordAt(position, fileSize);
                if (found.record().offset() != expectedOffset)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD, "segment offsets are not contiguous");
                retainedTail = found.record().data().producerStamp();
                position += found.bytes();
                expectedOffset++;
            }
            if (expectedOffset != offset)
                throw new CourseException(
                        ErrorCode.CORRUPT_RECORD, "truncate offset is not a record boundary");
            channel.truncate(position);
            channel.force(false);
            nextOffset = offset;
            tailProducerStamp = retainedTail;
        } catch (IOException exception) {
            throw storageError("truncate segment " + path, exception);
        } finally {
            lock.unlock();
        }
    }

    private RecordAt readRecordAt(long position, long fileSize) throws IOException {
        if (position < 0 || position >= fileSize || fileSize - position < Integer.BYTES)
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete record header");
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        ChannelIO.readFully(channel, lengthBuffer, position);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        if (length < RecordCodec.MIN_LENGTH || length > RecordCodec.MAX_LENGTH)
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "segment contains an invalid record length");
        int totalBytes = Integer.BYTES + length;
        if (fileSize - position < totalBytes)
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete record body");
        ByteBuffer encoded = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(length);
        ChannelIO.readFully(channel, encoded, position + Integer.BYTES);
        encoded.flip();
        LogRecord record = RecordCodec.decode(encoded);
        return new RecordAt(record, totalBytes);
    }

    private void ensureOpen() {
        if (closed)
            throw new CourseException(ErrorCode.STORAGE_ERROR, "segment is closed: " + path);
    }

    private static void validatePathAndBase(Path path, long baseOffset) {
        if (path == null || baseOffset < 0)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "segment path and base offset must be valid");
    }

    private static CourseException storageError(String action, IOException cause) {
        return new CourseException(ErrorCode.STORAGE_ERROR, "failed to " + action, cause);
    }

    /**
     * Closes the segment file channel; repeated calls have no effect. Open-state checks report
     * {@link ErrorCode#STORAGE_ERROR} after closure.
     *
     * @throws CourseException with {@link ErrorCode#STORAGE_ERROR} if closing the channel fails
     */
    @Override
    public void close() {
        lock.lock();
        try {
            if (closed) return;
            closed = true;
            channel.close();
        } catch (IOException exception) {
            throw storageError("close segment " + path, exception);
        } finally {
            lock.unlock();
        }
    }

    private record RecordAt(LogRecord record, int bytes) {}
}
