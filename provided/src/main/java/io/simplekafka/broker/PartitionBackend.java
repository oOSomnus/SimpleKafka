package io.simplekafka.broker;

import io.simplekafka.model.Acks;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import java.util.List;

public interface PartitionBackend {
    AppendResult produce(List<RecordData> records, Acks acks, int epoch, long timeoutMillis);
    List<LogRecord> fetch(long offset, int maxRecords, int maxBytes, int epoch);
    long logStartOffset();
    long logEndOffset();
    long highWatermark();
    int epoch();
}
