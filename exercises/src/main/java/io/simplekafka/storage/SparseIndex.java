package io.simplekafka.storage;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.ErrorCode;
import io.simplekafka.CourseException;
import io.simplekafka.model.IndexEntry;

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

    /** Step 5: persist the first and each interval-th offset/byte position in increasing order. See Step05Test and book step 5. */
    public synchronized void add(long offset, long position) {
        throw new ExerciseNotImplementedException(5, "SparseIndex.add");
    }

    /** Step 5: return the greatest indexed offset at or before the target, or the base/zero fallback. See Step05Test and book step 5. */
    public synchronized IndexEntry floor(long offset) {
        throw new ExerciseNotImplementedException(5, "SparseIndex.floor");
    }

    /** Step 5: rebuild ordered hints from every validated record in the segment; the log remains the source of truth. See Step05Test and book step 5. */
    public synchronized void rebuild(Path logFile, long baseOffset) {
        throw new ExerciseNotImplementedException(5, "SparseIndex.rebuild");
    }

    void reset() {
        ensureOpen();
        try {
            channel.truncate(0);
            entries.clear();
            lastObservedOffset = -1;
            lastObservedPosition = -1;
        } catch (IOException exception) {
            throw storageError("reset sparse index", exception);
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
}
