package io.simplekafka.model;

import java.util.Objects;

public record LogRecord(long offset, RecordData data) {
    public LogRecord {
        if (offset < 0) throw new IllegalArgumentException("offset must be nonnegative");
        Objects.requireNonNull(data, "data");
    }
}
