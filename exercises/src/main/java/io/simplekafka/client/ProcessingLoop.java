package io.simplekafka.client;

import io.simplekafka.ExerciseNotImplementedException;
import java.util.Objects;

/** Runs one poll/process/commit cycle with at-least-once delivery semantics. */
public final class ProcessingLoop {
    private final SimpleConsumer consumer;
    private final RecordProcessor processor;

    public ProcessingLoop(SimpleConsumer consumer, RecordProcessor processor) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    /**
     * Step 16: process a poll in partition/offset order and commit only after every callback succeeds.
     * Contract: propagate callback exceptions without committing, allowing replay on a rebuilt consumer.
     * Test: Step16Test. Lesson: docs/book/chapters/04-client-offset.tex, Step 16.
     */
    public int runOnce(int maxRecords, int maxBytes) throws Exception {
        throw new ExerciseNotImplementedException(16, "runOnce");
    }
}
