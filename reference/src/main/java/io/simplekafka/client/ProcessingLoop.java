package io.simplekafka.client;

import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/** Runs one poll/process/commit cycle with at-least-once delivery semantics. */
public final class ProcessingLoop {
    private final SimpleConsumer consumer;
    private final RecordProcessor processor;
    private boolean failed;

    public ProcessingLoop(SimpleConsumer consumer, RecordProcessor processor) {
        this.consumer = Objects.requireNonNull(consumer, "consumer");
        this.processor = Objects.requireNonNull(processor, "processor");
    }

    public int runOnce(int maxRecords, int maxBytes) throws Exception {
        if (failed)
            throw new IllegalStateException(
                    "processing loop failed; close and rebuild its consumer");
        try {
            Map<TopicPartition, List<LogRecord>> polled = consumer.poll(maxRecords, maxBytes);
            int processed = 0;
            for (Map.Entry<TopicPartition, List<LogRecord>> partition :
                    new TreeMap<>(polled).entrySet()) {
                for (LogRecord record : partition.getValue()) {
                    processor.process(partition.getKey(), record);
                    processed++;
                }
            }
            consumer.commitSync();
            return processed;
        } catch (Exception | Error failure) {
            failed = true;
            throw failure;
        }
    }
}
