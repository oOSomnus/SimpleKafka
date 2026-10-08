package io.simplekafka.model;

import java.util.Arrays;
import java.util.Objects;

/** The caller transfers ownership of key and value arrays at construction. */
public record RecordData(byte[] key, byte[] value, long timestamp) {
    public RecordData {
        Objects.requireNonNull(value, "value");
        if (timestamp < 0) throw new IllegalArgumentException("timestamp must be nonnegative");
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof RecordData data
                        && timestamp == data.timestamp
                        && Arrays.equals(key, data.key)
                        && Arrays.equals(value, data.value);
    }

    @Override
    public int hashCode() {
        int result = Arrays.hashCode(key);
        result = 31 * result + Arrays.hashCode(value);
        return 31 * result + Long.hashCode(timestamp);
    }
}
