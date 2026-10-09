package io.simplekafka.broker;

import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.RecordData;

import java.util.List;

/** Optional broker capability for durable idempotent producer batches. */
public interface IdempotentProduceBackend {
    AppendResult produceIdempotent(
            List<RecordData> records,
            Acks acks,
            int leaderEpoch,
            long timeoutMillis,
            long producerId,
            int producerEpoch,
            long firstSequence);
}
