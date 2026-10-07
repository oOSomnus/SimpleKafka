package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.IndexEntry;
import io.simplekafka.model.LogRecord;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.channels.FileChannel;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.Map;
import java.util.TreeMap;

/** Sparse, rebuildable byte-position hints for a segment. */
public final class SparseIndex implements AutoCloseable {
    private static final int ENTRY_BYTES = Long.BYTES * 2;

    private final Path path;
    private final long baseOffset;
    private final int intervalRecords;
    private final TreeMap<Long, Long> entries = new TreeMap<>();
    private FileChannel channel;
    private long lastObservedOffset = -1;
    private long lastObservedPosition = -1;
    private boolean closed;

    public SparseIndex(Path path, long baseOffset, int intervalRecords) {
        if (path == null || baseOffset < 0 || intervalRecords <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid sparse index configuration");
        this.path = path;
        this.baseOffset = baseOffset;
        this.intervalRecords = intervalRecords;
        try {
            Path parent = path.getParent();
            if (parent != null) Files.createDirectories(parent);
            channel = FileChannel.open(path, StandardOpenOption.CREATE, StandardOpenOption.READ, StandardOpenOption.WRITE);
            loadIfValid();
        } catch (IOException exception) {
            if (channel != null) {
                try { channel.close(); } catch (IOException closeFailure) { exception.addSuppressed(closeFailure); }
            }
            throw storageError("open sparse index", exception);
        }
    }

    public synchronized void add(long offset, long position) {
        ensureOpen();
        if (offset < baseOffset || position < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "index offset and position must be nonnegative and in range");
        if (lastObservedOffset < 0) {
            if (offset != baseOffset || position != 0)
                throw new CourseException(ErrorCode.INVALID_REQUEST, "the first sparse index observation must be the segment start");
        } else if (offset <= lastObservedOffset || position <= lastObservedPosition) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "sparse index entries must increase by offset and position");
        }
        lastObservedOffset = offset;
        lastObservedPosition = position;
        if (!entries.isEmpty() && (offset - baseOffset) % intervalRecords != 0) return;

        ByteBuffer encoded = ByteBuffer.allocate(ENTRY_BYTES).order(ByteOrder.BIG_ENDIAN);
        encoded.putLong(offset).putLong(position).flip();
        try {
            long filePosition = channel.size();
            ChannelIO.writeFully(channel, encoded, filePosition);
            entries.put(offset, position);
        } catch (IOException exception) {
            throw storageError("append sparse index", exception);
        }
    }

    public synchronized IndexEntry floor(long offset) {
        ensureOpen();
        Map.Entry<Long, Long> entry = entries.floorEntry(offset);
        return entry == null ? new IndexEntry(baseOffset, 0) : new IndexEntry(entry.getKey(), entry.getValue());
    }

    public synchronized void rebuild(Path logFile, long requestedBaseOffset) {
        ensureOpen();
        if (logFile == null || requestedBaseOffset != baseOffset)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "sparse index rebuild base does not match its segment");
        clearEntries();
        try (FileChannel log = FileChannel.open(logFile, StandardOpenOption.READ)) {
            long fileSize = log.size();
            long position = 0;
            long expectedOffset = baseOffset;
            while (position < fileSize) {
                RecordAt found = readRecordAt(log, position, fileSize);
                if (found.record().offset() != expectedOffset)
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offsets are not contiguous while rebuilding its index");
                add(expectedOffset, position);
                position += found.bytes();
                try {
                    expectedOffset = Math.addExact(expectedOffset, 1L);
                } catch (ArithmeticException exception) {
                    throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment offset overflow while rebuilding its index", exception);
                }
            }
        } catch (IOException exception) {
            throw storageError("rebuild sparse index from " + logFile, exception);
        }
    }

    private void loadIfValid() throws IOException {
        long size = channel.size();
        boolean valid = size % ENTRY_BYTES == 0;
        long previousOffset = -1;
        long previousPosition = -1;
        if (valid) {
            for (long position = 0; position < size; position += ENTRY_BYTES) {
                ByteBuffer entry = ByteBuffer.allocate(ENTRY_BYTES).order(ByteOrder.BIG_ENDIAN);
                ChannelIO.readFully(channel, entry, position);
                entry.flip();
                long offset = entry.getLong();
                long bytePosition = entry.getLong();
                boolean first = entries.isEmpty();
                if (offset < baseOffset || bytePosition < 0
                        || (first && (offset != baseOffset || bytePosition != 0))
                        || (!first && (offset - previousOffset != intervalRecords
                        || bytePosition <= previousPosition))) {
                    valid = false;
                    break;
                }
                entries.put(offset, bytePosition);
                previousOffset = offset;
                previousPosition = bytePosition;
            }
        }
        if (!valid) {
            entries.clear();
            channel.truncate(0);
        } else if (!entries.isEmpty()) {
            Map.Entry<Long, Long> last = entries.lastEntry();
            lastObservedOffset = last.getKey();
            lastObservedPosition = last.getValue();
        }
    }

    void reset() { clearEntries(); }

    private RecordAt readRecordAt(FileChannel log, long position, long fileSize) throws IOException {
        if (fileSize - position < Integer.BYTES)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete index-rebuild header");
        ByteBuffer lengthBuffer = ByteBuffer.allocate(Integer.BYTES).order(ByteOrder.BIG_ENDIAN);
        ChannelIO.readFully(log, lengthBuffer, position);
        lengthBuffer.flip();
        int length = lengthBuffer.getInt();
        if (length < RecordCodec.MIN_LENGTH || length > RecordCodec.MAX_LENGTH)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment has an invalid record length during index rebuild");
        int bytes = Integer.BYTES + length;
        if (fileSize - position < bytes)
            throw new CourseException(ErrorCode.CORRUPT_RECORD, "segment ends in an incomplete index-rebuild body");
        ByteBuffer encoded = ByteBuffer.allocate(bytes).order(ByteOrder.BIG_ENDIAN);
        encoded.putInt(length);
        ChannelIO.readFully(log, encoded, position + Integer.BYTES);
        encoded.flip();
        LogRecord record = RecordCodec.decode(encoded);
        return new RecordAt(record, bytes);
    }

    private void clearEntries() {
        try {
            channel.truncate(0);
        } catch (IOException exception) {
            throw storageError("clear sparse index", exception);
        }
        entries.clear();
        lastObservedOffset = -1;
        lastObservedPosition = -1;
    }

    Path path() { return path; }
    long baseOffset() { return baseOffset; }

    private void ensureOpen() {
        if (closed) throw new CourseException(ErrorCode.STORAGE_ERROR, "sparse index is closed");
    }

    private static CourseException storageError(String action, IOException cause) {
        return new CourseException(ErrorCode.STORAGE_ERROR, "failed to " + action, cause);
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        try {
            channel.close();
        } catch (IOException exception) {
            throw storageError("close sparse index", exception);
        }
    }

    private record RecordAt(LogRecord record, int bytes) { }
}
