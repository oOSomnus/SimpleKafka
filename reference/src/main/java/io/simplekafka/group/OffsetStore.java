package io.simplekafka.group;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
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

    public synchronized void commit(OffsetKey key, long nextOffset) {
        ensureOpen();
        if (key == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "offset key must not be null");
        if (nextOffset < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "nextOffset must be nonnegative");
        byte[] encoded = MessageCodec.encodeOffsetEntry(key, nextOffset);
        log.append(List.of(new RecordData(null, encoded, 0)));
        committed.put(key, nextOffset);
    }

    public synchronized OptionalLong fetch(OffsetKey key) {
        ensureOpen();
        if (key == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "offset key must not be null");
        Long offset = committed.get(key);
        return offset == null ? OptionalLong.empty() : OptionalLong.of(offset);
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

    private void ensureOpen() {
        if (closed) throw new CourseException(ErrorCode.STORAGE_ERROR, "offset store is closed");
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        committed.clear();
        log.close();
    }
}
