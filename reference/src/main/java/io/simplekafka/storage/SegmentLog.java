package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
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
import java.util.ArrayList;
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
            try { segment.close(); } catch (RuntimeException closeFailure) { exception.addSuppressed(closeFailure); }
            throw exception;
        }
    }

    public AppendResult append(List<RecordData> records) {
        if (records == null || records.isEmpty())
            throw new CourseException(ErrorCode.INVALID_REQUEST, "append batch must not be empty");
        lock.lock();
        try {
            ensureOpen();
            long firstOffset = nextOffset;
            long candidateNext = firstOffset;
            ArrayList<ByteBuffer> encoded = new ArrayList<>(records.size());
            for (RecordData data : records) {
                RecordCodec.encodedSize(data);
                LogRecord record = new LogRecord(candidateNext, data);
                encoded.add(RecordCodec.encode(record));
                try {
                    candidateNext = Math.addExact(candidateNext, 1L);
                } catch (ArithmeticException exception) {
                    throw new CourseException(ErrorCode.INVALID_REQUEST, "log end offset overflow", exception);
                }
            }
            long position = channel.size();
            for (ByteBuffer record : encoded) {
                int recordBytes = record.remaining();
                ChannelIO.writeFully(channel, record, position);
                position += recordBytes;
            }
            channel.force(false);
            nextOffset = candidateNext;
            return new AppendResult(firstOffset, candidateNext);
        } catch (IOException exception) {
            throw storageError("append to segment " + path, exception);
        } finally {
            lock.unlock();
        }
    }

    public List<LogRecord> read(long offset, int maxRecords, int maxBytes) {
        return readFrom(offset, 0, maxRecords, maxBytes);
    }

    public List<LogRecord> readFrom(long offset, long bytePositionHint, int maxRecords, int maxBytes) {
        if (maxRecords <= 0 || maxBytes <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "read limits must be positive");
        lock.lock();
        try {
            ensureOpen();
            if (offset < baseOffset || offset > nextOffset)
                throw new CourseException(ErrorCode.OFFSET_OUT_OF_RANGE, "offset is outside this segment");
            if (bytePositionHint < 0)
                throw new CourseException(ErrorCode.INVALID_REQUEST, "byte-position hint must be nonnegative");
            long fileSize = channel.size();
            Hint hint = validateHint(bytePositionHint, offset, fileSize);
            long position = hint.position();
            if (offset == nextOffset) return List.of();

            ArrayList<LogRecord> result = new ArrayList<>(Math.min(maxRecords, 16));
            long expectedOffset = hint.offset();
            long totalBytes = 0;
            while (position < fileSize && result.size() < maxRecords) {
                RecordAt found = readRecordAt(position, fileSize);
                long recordOffset = found.record().offset();
                if (recordOffset != expectedOffset)
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offsets are not contiguous");
                expectedOffset++;
                if (recordOffset < offset) {
                    position += found.bytes();
                    continue;
                }
                if (recordOffset > offset && result.isEmpty())
                    throw new CourseException(ErrorCode.INVALID_REQUEST, "byte-position hint skipped the requested offset");
                long nextBytes = totalBytes + found.bytes();
                if (nextBytes > maxBytes) break;
                result.add(found.record());
                totalBytes = nextBytes;
                position += found.bytes();
            }
            return List.copyOf(result);
        } catch (IOException exception) {
            throw storageError("read segment " + path, exception);
        } finally {
            lock.unlock();
        }
    }

    public long recover() {
        lock.lock();
        try {
            ensureOpen();
            long fileSize = channel.size();
            long position = 0;
            long expectedOffset = baseOffset;
            while (position < fileSize) {
                long remaining = fileSize - position;
                if (remaining < Integer.BYTES) {
                    truncateTail(position);
                    break;
                }
                ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
                ChannelIO.readFully(channel, lengthBuffer, position);
                lengthBuffer.flip();
                int length = lengthBuffer.getInt();
                if (length < RecordCodec.MIN_LENGTH || length > RecordCodec.MAX_LENGTH)
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment contains an invalid record length");
                long recordBytes = Integer.BYTES + (long) length;
                if (remaining < recordBytes) {
                    truncateTail(position);
                    break;
                }
                RecordAt found = readRecordAt(position, fileSize);
                if (found.record().offset() != expectedOffset)
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offsets are not contiguous");
                position += found.bytes();
                try {
                    expectedOffset = Math.addExact(expectedOffset, 1L);
                } catch (ArithmeticException exception) {
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offset overflow", exception);
                }
            }
            nextOffset = expectedOffset;
            return nextOffset;
        } catch (IOException exception) {
            throw storageError("recover segment " + path, exception);
        } finally {
            lock.unlock();
        }
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
            while (position < fileSize) {
                if (expectedOffset == offset) break;
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

    private Hint validateHint(long hint, long requestedOffset, long fileSize) throws IOException {
        if (hint > fileSize)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "byte-position hint exceeds segment size");
        long position = 0;
        long expectedOffset = baseOffset;
        while (position < hint) {
            RecordAt found = readRecordAt(position, fileSize);
            if (found.record().offset() != expectedOffset)
                throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offsets are not contiguous");
            position += found.bytes();
            expectedOffset++;
            if (position > hint)
                throw new CourseException(ErrorCode.INVALID_REQUEST, "byte-position hint is not a record boundary");
        }
        if (position != hint || expectedOffset > requestedOffset)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "byte-position hint skips the requested offset");
        return new Hint(position, expectedOffset);
    }

    private RecordAt readRecordAt(long position, long fileSize) throws IOException {
        if (position < 0 || position >= fileSize || fileSize - position < Integer.BYTES)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete record header");
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        ChannelIO.readFully(channel, lengthBuffer, position);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        if (length < RecordCodec.MIN_LENGTH || length > RecordCodec.MAX_LENGTH)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment contains an invalid record length");
        int totalBytes = Integer.BYTES + length;
        if (fileSize - position < totalBytes)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete record body");
        ByteBuffer encoded = ByteBuffer.allocate(totalBytes).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(length);
        ChannelIO.readFully(channel, encoded, position + Integer.BYTES);
        encoded.flip();
        LogRecord record = RecordCodec.decode(encoded);
        return new RecordAt(record, totalBytes);
    }

    private void truncateTail(long position) throws IOException {
        channel.truncate(position);
        channel.force(false);
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
    private record Hint(long position, long offset) { }
}
