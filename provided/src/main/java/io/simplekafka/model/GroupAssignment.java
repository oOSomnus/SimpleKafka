package io.simplekafka.model;

import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * The partition assignment for each member in one group generation.
 *
 * <p>The constructor copies the map and its partition lists; the resulting map is immutable and
 * ordered by member key. Member keys are not checked against topic identifier syntax or a character
 * set; only null keys are rejected.
 *
 * @param generation nonnegative group generation
 * @param assignments map from member identifiers to assigned partitions
 */
public record GroupAssignment(int generation, Map<String, List<TopicPartition>> assignments) {
    /**
     * Creates an assignment with sorted, immutable copies of the supplied map and lists.
     *
     * @param generation group generation
     * @param assignments map from member identifiers to assigned partitions
     * @throws NullPointerException if the map, a member key, a partition list, or a list element is
     *     null
     * @throws IllegalArgumentException if {@code generation} is negative
     */
    public GroupAssignment {
        if (generation < 0) throw new IllegalArgumentException("generation must be nonnegative");
        Objects.requireNonNull(assignments, "assignments");
        TreeMap<String, List<TopicPartition>> copy = new TreeMap<>();
        assignments.forEach(
                (member, partitions) ->
                        copy.put(Objects.requireNonNull(member), List.copyOf(partitions)));
        assignments = Collections.unmodifiableMap(copy);
    }
}
