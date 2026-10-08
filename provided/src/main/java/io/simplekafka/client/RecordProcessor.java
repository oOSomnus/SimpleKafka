package io.simplekafka.client;

import io.simplekafka.model.LogRecord;
import io.simplekafka.model.TopicPartition;

/**
 * Callback for processing records retrieved by a {@link ProcessingLoop}.
 *
 * <p>The loop invokes callbacks in partition and offset order and commits only after every callback
 * in the poll succeeds. A failed callback is propagated without committing that poll, so its side
 * effects may be repeated when the records are retried.
 */
@FunctionalInterface
public interface RecordProcessor {
    /**
     * Processes one fetched record.
     *
     * @param tp partition containing the record
     * @param record record to process
     * @throws Exception if processing fails; {@link ProcessingLoop} propagates the exception and
     *     does not commit the poll
     */
    void process(TopicPartition tp, LogRecord record) throws Exception;
}
