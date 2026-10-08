package io.simplekafka.client;

import io.simplekafka.ExerciseNotImplementedException;

import java.util.Objects;

/** Runs one poll/process/commit cycle with at-least-once delivery semantics. */
public final class ProcessingLoop {
    private final SimpleConsumer consumer;
    private final RecordProcessor processor;

    /**
     * Creates a processing loop for one consumer and record callback.
     *
     * @param consumer consumer used to poll and commit positions
     * @param processor callback invoked for each record
     * @throws NullPointerException if {@code consumer} or {@code processor} is null
     */
    public ProcessingLoop(SimpleConsumer consumer, RecordProcessor processor) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    /**
     * Step 16: process a poll in ascending partition and offset order and commit only after every
     * callback succeeds. A callback failure propagates unchanged, commits nothing, and poisons this
     * loop; rebuilding the consumer and loop permits at-least-once redelivery. Test: Step16Test.
     * Lesson: docs/book/chapters/04-client-offset.tex, Step 16.
     *
     * @param maxRecords positive total record budget for the poll
     * @param maxBytes positive total encoded-byte budget for the poll
     * @return the number of records processed and committed
     * @throws IllegalStateException if a prior callback failure poisoned this loop
     * @throws Exception if polling, processing a callback, or committing fails
     * @throws ExerciseNotImplementedException while the Step 16 exercise method is a skeleton
     */
    public int runOnce(int maxRecords, int maxBytes) throws Exception {
        throw new ExerciseNotImplementedException(16, "runOnce");
    }
}
