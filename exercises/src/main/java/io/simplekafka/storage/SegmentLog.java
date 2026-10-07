package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.FileAlreadyExistsException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.locks.ReentrantLock;

/** One append-only, offset-contiguous on-disk log segment. */
public final class SegmentLog implements AutoCloseable {
    private final Path path;
    private final FileChannel channel;
    private final ReentrantLock lock = new ReentrantLock();
    private final long baseOffset;
    private long nextOffset;
    private boolean closed;

    private SegmentLog(Path path, FileChannel channel, long baseOffset) {
        this.path = path;
        this.channel = channel;
        this.baseOffset = baseOffset;
        this.nextOffset = baseOffset;
    }

    public static SegmentLog create(Path path, long baseOffset) {
        validatePathAndBase(path, baseOffset);
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            FileChannel channel = FileChannel.open(path, StandardOpenOption.CREATE_NEW,
                    StandardOpenOption.READ, StandardOpenOption.WRITE);
            return new SegmentLog(path, channel, baseOffset);
        } catch (FileAlreadyExistsException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "segment already exists: " + path, exception);
        } catch (IOException exception) {
            throw storageError("create segment " + path, exception);
        }
    }

    public static SegmentLog open(Path path, long baseOffset) {
        validatePathAndBase(path, baseOffset);
        try {
            FileChannel channel = FileChannel.open(path, StandardOpenOption.READ, StandardOpenOption.WRITE);
            SegmentLog segment = new SegmentLog(path, channel, baseOffset);
            try {
                segment.recover();
                return segment;
            } catch (RuntimeException exception) {
                try { segment.close(); } catch (RuntimeException closeFailure) { exception.addSuppressed(closeFailure); }
                throw exception;
            }
        } catch (IOException exception) {
            throw storageError("open segment " + path, exception);
        }
    }

    /** Step 2: prevalidate and append a nonempty batch at consecutive offsets, force it, then return its half-open offset range. See Step02Test and book step 2. */
    public AppendResult append(List<RecordData> records) {
        throw new ExerciseNotImplementedException(2, "SegmentLog.append");
    }

    public List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
        return readFrom(offset, 0, maxRecords, maxBytes);
    }

    /** Step 3: read complete records within both limits from a verified record-boundary hint without changing append position. See Step03Test and book step 3. */
    public List<LogRecord> readFrom(long offset, long bytePositionHint, int maxRecords, int maxBytes) {
        throw new ExerciseNotImplementedException(3, "SegmentLog.readFrom");
    }

    /** Step 4: scan contiguous valid records, truncate only an incomplete tail, and return the recovered LEO. See Step04Test and book step 4. */
    public long recover() {
        throw new ExerciseNotImplementedException(4, "SegmentLog.recover");
    }

    public long baseOffset() { return baseOffset; }

    public long logEndOffset() {
        lock.lock();
        try {
            ensureOpen();
            return nextOffset;
        } finally {
            lock.unlock();
        }
    }

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

    public Path path() { return path; }

    void truncateToOffset(long offset) {
        lock.lock();
        try {
            ensureOpen();
            if (offset < baseOffset || offset > nextOffset)
                throw new CourseException(ErrorCode.OFFSET_OUT_OF_RANGE, "truncate offset is outside this segment");
            if (offset == nextOffset) return;
            long fileSize = channel.size();
            long position = 0;
            long expectedOffset = baseOffset;
            while (position < fileSize && expectedOffset < offset) {
                RecordAt found = readRecordAt(position, fileSize);
                if (found.record().offset() != expectedOffset)
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offsets are not contiguous");
                position += found.bytes();
                expectedOffset++;
            }
            if (expectedOffset != offset)
                throw new CourseException(ErrorCode.CORRUPT_RECORD, "truncate offset is not a record boundary");
            channel.truncate(position);
            channel.force(false);
            nextOffset = offset;
        } catch (IOException exception) {
            throw storageError("truncate segment " + path, exception);
        } finally {
            lock.unlock();
        }
    }

    private RecordAt readRecordAt(long position, long fileSize) throws IOException {
        if (fileSize - position < Integer.BYTES)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete record header");
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        ChannelIO.readFully(channel, lengthBuffer, position);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        if (length < RecordCodec.MIN_LENGTH || length > RecordCodec.MAX_LENGTH)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment contains an invalid record length");
        int bytes = Integer.BYTES + length;
        if (fileSize - position < bytes)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete record body");
        ByteBuffer encoded = ByteBuffer.allocate(bytes).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(length);
        ChannelIO.readFully(channel, encoded, position + Integer.BYTES);
        encoded.flip();
        return new RecordAt(RecordCodec.decode(encoded), bytes);
    }

    private void ensureOpen() {
        if (closed) throw new CourseException(ErrorCode.STORAGE_ERROR, "segment is closed: " + path);
    }

    private static void validatePathAndBase(Path path, long baseOffset) {
        if (path == null || baseOffset < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "segment path and base offset must be valid");
    }

    private static CourseException storageError(String action, IOException cause) {
        return new CourseException(ErrorCode.STORAGE_ERROR, "failed to " + action, cause);
    }

    @Override public void close() {
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
    private record RecordAt(LogRecord record, int bytes) { }
}
