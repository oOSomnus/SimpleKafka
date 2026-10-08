package io.simplekafka.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * Identifies one partition of a topic.
 *
 * @param topic topic identifier containing 1 to 255 ASCII characters from
 *     {@code [A-Za-z0-9._-]}, except {@code "."} and {@code ".."}
 * @param partition nonnegative partition number
 */
public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {
    /**
     * Creates a topic-partition after validating the topic identifier and partition number.
     *
     * @param topic topic identifier
     * @param partition partition number
     * @throws NullPointerException if {@code topic} is null
     * @throws IllegalArgumentException if the topic is empty, too long, outside the allowed ASCII
     *     set, equal to {@code "."} or {@code ".."}, or if {@code partition} is negative
     */
    public TopicPartition {
        Objects.requireNonNull(topic, "topic");
        byte[] bytes = topic.getBytes(StandardCharsets.UTF_8);
        if (bytes.length == 0
                || bytes.length > 255
                || !topic.matches("[A-Za-z0-9._-]+")
                || topic.equals(".")
                || topic.equals(".."))
            throw new IllegalArgumentException("invalid topic identifier");
        if (partition < 0) throw new IllegalArgumentException("partition must be nonnegative");
    }

    /**
     * Compares topic-partitions by topic and then by partition number.
     *
     * @param other topic-partition to compare with this one
     * @return a negative value, zero, or a positive value as this value sorts before, equal to, or
     *     after {@code other}
     * @throws NullPointerException if {@code other} is null
     */
    @Override
    public int compareTo(TopicPartition other) {
        int byTopic = topic.compareTo(other.topic);
        return byTopic != 0 ? byTopic : Integer.compare(partition, other.partition);
    }

    /**
     * Returns the topic followed by a hyphen and this partition number.
     *
     * @return this topic-partition in {@code topic-partition} form
     */
    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
