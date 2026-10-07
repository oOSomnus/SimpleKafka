package io.simplekafka.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public record OffsetKey(String group, TopicPartition tp) implements Comparable<OffsetKey> {
    public OffsetKey {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(tp, "tp");
        byte[] bytes = group.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 255) throw new IllegalArgumentException("group must contain 1..255 UTF-8 bytes");
    }
    @Override public int compareTo(OffsetKey other) {
        int byGroup = group.compareTo(other.group);
        return byGroup != 0 ? byGroup : tp.compareTo(other.tp);
    }
}
