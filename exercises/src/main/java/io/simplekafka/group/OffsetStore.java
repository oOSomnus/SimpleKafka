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

    public OffsetStore(Path file) {
        this.log = new PartitionLog(Objects.requireNonNull(file, "file"), SEGMENT_BYTES, 1);
    }

    /**
     * Step 15: append the group's next offset to the durable store. Contract: use
     * MessageCodec.encodeOffsetEntry, null key and timestamp zero; values may move backward. Test:
     * Step15Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 15.
     */
    public void commit(OffsetKey key, long nextOffset) {
        throw new ExerciseNotImplementedException(15, "commit");
    }

    /**
     * Step 15: return the last committed next offset for this group/partition pair. Contract:
     * absent values are OptionalLong.empty; replay survives store reopen. Test: Step15Test. Lesson:
     * docs/book/chapters/04-client-offset.tex, Step 15.
     */
    public OptionalLong fetch(OffsetKey key) {
        throw new ExerciseNotImplementedException(15, "fetch");
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        log.close();
    }
}
