package io.simplekafka.replication;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import java.util.Objects;

/** Step 23: validates and waits for the configured produce acknowledgement level. */
public final class AckPolicy {
    private final ReplicationTracker tracker;

    public AckPolicy(ReplicationTracker tracker) { this.tracker = Objects.requireNonNull(tracker); }

    /** Step 23: reject ALL before append when ISR is below minISR. See Step23Test and book step 23. */
    public void validateBeforeAppend(Acks acks) {
        throw new ExerciseNotImplementedException(23, "AckPolicy.validateBeforeAppend");
    }

    /** Step 23: wait for the captured epoch's high watermark until the absolute deadline. See Step23Test and book step 23. */
    public void await(AppendResult result, Acks acks, int epoch, long deadlineNanos) {
        throw new ExerciseNotImplementedException(23, "AckPolicy.await");
    }
}
