package io.simplekafka.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Identifies a committed offset by group and topic-partition.
 *
 * <p>The group identifier is limited by UTF-8 byte length and is not subject to the topic
 * identifier's ASCII character restrictions.
 *
 * @param group non-null group identifier of 1 to 255 UTF-8 bytes
 * @param tp non-null topic-partition whose committed position is identified
 */
public record OffsetKey(String group, TopicPartition tp) implements Comparable<OffsetKey> {
    /**
     * Creates a group offset key after validating its group identifier.
     *
     * @param group group identifier
     * @param tp topic-partition
     * @throws NullPointerException if {@code group} or {@code tp} is null
     * @throws IllegalArgumentException if {@code group} is empty or longer than 255 UTF-8 bytes
     */
    public OffsetKey {
        Objects.requireNonNull(group, "group");
        Objects.requireNonNull(tp, "tp");
        byte[] bytes = group.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0 || bytes.length > 255)
            throw new IllegalArgumentException("group must contain 1..255 UTF-8 bytes");
    }

    /**
     * Compares offset keys by group identifier and then by topic-partition.
     *
     * @param other offset key to compare with this one
     * @return a negative value, zero, or a positive value as this value sorts before, equal to, or
     *     after {@code other}
     * @throws NullPointerException if {@code other} is null
     */
    @Override
    public int compareTo(OffsetKey other) {
        int byGroup = group.compareTo(other.group);
        return byGroup != 0 ? byGroup : tp.compareTo(other.tp);
    }
}
