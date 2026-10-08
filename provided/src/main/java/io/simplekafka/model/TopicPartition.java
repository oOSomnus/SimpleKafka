package io.simplekafka.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public record TopicPartition(String topic, int partition) implements Comparable<TopicPartition> {
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

    @Override
    public int compareTo(TopicPartition other) {
        int byTopic = topic.compareTo(other.topic);
        return byTopic != 0 ? byTopic : Integer.compare(partition, other.partition);
    }

    @Override
    public String toString() {
        return topic + "-" + partition;
    }
}
