package io.simplekafka.lab;

import io.simplekafka.client.RecordProcessor;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/** Records every callback as an externally observable processing side effect. */
public final class EffectRecorder implements RecordProcessor {
    private final ArrayList<LogRecord> records = new ArrayList<>();

    /** Adds the actual record delivered to the callback, including intentional duplicates. */
    @Override
    public synchronized void process(TopicPartition tp, LogRecord record) {
        Objects.requireNonNull(tp, "tp");
        records.add(Objects.requireNonNull(record, "record"));
    }

    /** Returns a stable snapshot of callback records in invocation order. */
    public synchronized List<LogRecord> snapshot() {
        return List.copyOf(records);
    }
}
