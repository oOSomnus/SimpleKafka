package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;

import java.util.List;

/** Step 30: derive idempotent producer state from the retained partition log. */
public final class IdempotentAppender {
    private final PartitionLog log;

    public IdempotentAppender(PartitionLog log) {
        if (log == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "partition log is null");
        this.log = log;
    }

    /** Appends one sequenced batch or returns the original range for an exact retry. */
    public AppendResult append(
            long producerId, int producerEpoch, long firstSequence, List<RecordData> records) {
        throw new ExerciseNotImplementedException(30, "IdempotentAppender.append");
    }

    /** Reconstructs sequence and duplicate-result state from retained stamped log records. */
    public void recover() {
        throw new ExerciseNotImplementedException(30, "IdempotentAppender.recover");
    }
}
