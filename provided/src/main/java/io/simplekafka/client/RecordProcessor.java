package io.simplekafka.client;

import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;

@FunctionalInterface
public interface RecordProcessor {
    void process(TopicPartition tp, LogRecord record) throws Exception;
}
