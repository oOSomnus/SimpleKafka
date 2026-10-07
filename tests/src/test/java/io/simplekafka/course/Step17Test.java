package io.simplekafka.course;

import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.TopicPartition;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step17Test {
    @Test
    void sortsAndDeduplicatesPartitionsAndMembersBeforeRoundRobinAssignment() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();
        List<TopicPartition> input = List.of(
                new TopicPartition("orders", 4), new TopicPartition("orders", 1),
                new TopicPartition("orders", 2), new TopicPartition("orders", 0),
                new TopicPartition("orders", 3), new TopicPartition("orders", 0));

        Map<String, List<TopicPartition>> assigned = assignor.assign(input, List.of("b", "a", "a"));
        assertEquals(Map.of(
                "a", List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 2), new TopicPartition("orders", 4)),
                "b", List.of(new TopicPartition("orders", 1), new TopicPartition("orders", 3))), assigned);
        assertEquals(5, assigned.values().stream().mapToInt(List::size).sum());
        assertTrue(Math.abs(assigned.get("a").size() - assigned.get("b").size()) <= 1);
    }

    @Test
    void retainsEmptyMemberAssignmentsAndHandlesNoMembers() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();
        List<TopicPartition> five = List.of(
                new TopicPartition("orders", 4), new TopicPartition("orders", 3),
                new TopicPartition("orders", 2), new TopicPartition("orders", 1),
                new TopicPartition("orders", 0));
        Map<String, List<TopicPartition>> assigned = assignor.assign(five,
                List.of("e", "d", "c", "b", "a", "unused-1", "unused-0"));
        assertEquals(7, assigned.size());
        assertEquals(5, assigned.values().stream().filter(list -> list.size() == 1).count());
        assertEquals(2, assigned.values().stream().filter(List::isEmpty).count());
        assertEquals(Map.of(), assignor.assign(five, List.of()));
        assertEquals(Map.of("a", List.of(), "b", List.of()), assignor.assign(List.of(), List.of("b", "a")));
    }
}
