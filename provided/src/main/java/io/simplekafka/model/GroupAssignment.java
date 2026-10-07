package io.simplekafka.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

public record GroupAssignment(int generation, Map<String, List<TopicPartition>> assignments) {
    public GroupAssignment {
        if (generation < 0) throw new IllegalArgumentException("generation must be nonnegative");
        Objects.requireNonNull(assignments, "assignments");
        TreeMap<String, List<TopicPartition>> copy = new TreeMap<>();
        assignments.forEach((member, partitions) -> copy.put(Objects.requireNonNull(member), List.copyOf(partitions)));
        assignments = Collections.unmodifiableMap(copy);
    }
}
