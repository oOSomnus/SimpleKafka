package io.simplekafka.group;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.TopicPartition;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.TreeSet;

/** Deterministically assigns sorted partitions across sorted member identifiers. */
public final class RoundRobinAssignor {
    public RoundRobinAssignor() {}

    public Map<String, List<TopicPartition>> assign(
            List<TopicPartition> partitions, List<String> members) {
        if (partitions == null || members == null)
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "partitions and members must not be null");
        TreeSet<TopicPartition> sortedPartitions = new TreeSet<>();
        TreeSet<String> sortedMembers = new TreeSet<>();
        try {
            for (TopicPartition partition : partitions)
                sortedPartitions.add(Objects.requireNonNull(partition));
            for (String member : members) {
                Objects.requireNonNull(member);
                int utf8Length = member.getBytes(StandardCharsets.UTF_8).length;
                if (utf8Length == 0 || utf8Length > 255)
                    throw new IllegalArgumentException("invalid member identifier");
                sortedMembers.add(member);
            }
        } catch (RuntimeException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid assignment input", exception);
        }
        if (sortedMembers.isEmpty()) return Map.of();
        List<String> memberOrder = List.copyOf(sortedMembers);
        TreeMap<String, List<TopicPartition>> assignments = new TreeMap<>();
        for (String member : memberOrder) assignments.put(member, new ArrayList<>());
        int index = 0;
        for (TopicPartition partition : sortedPartitions) {
            assignments.get(memberOrder.get(index % memberOrder.size())).add(partition);
            index++;
        }
        TreeMap<String, List<TopicPartition>> immutable = new TreeMap<>();
        assignments.forEach((member, owned) -> immutable.put(member, List.copyOf(owned)));
        return Collections.unmodifiableMap(immutable);
    }
}
