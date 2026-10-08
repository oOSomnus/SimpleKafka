package io.simplekafka.course;

import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.TopicPartition;
import java.util.List;
import java.util.Map;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step17Test {
    @Test
    @DisplayName("Sorts and deduplicates partitions and members before round robin assignment")
    void sortsAndDeduplicatesPartitionsAndMembersBeforeRoundRobinAssignment() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();
        List<TopicPartition> input = List.of(
                new TopicPartition("orders", 4), new TopicPartition("orders", 1),
                new TopicPartition("orders", 2), new TopicPartition("orders", 0),
                new TopicPartition("orders", 3), new TopicPartition("orders", 0));

        Map<String, List<TopicPartition>> expected = Map.of(
                "a", List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 2),
                        new TopicPartition("orders", 4)),
                "b", List.of(new TopicPartition("orders", 1), new TopicPartition("orders", 3)));
        assertEquals(expected, assignor.assign(input, List.of("b", "a", "a")));
        assertEquals(expected, assignor.assign(input, List.of("b", "a", "a")));
    }


    @Test
    @DisplayName("Assigns five partitions to the first five of seven sorted members and keeps empty members")
    void assignsFivePartitionsToTheFirstFiveOfSevenSortedMembersAndKeepsEmptyMembers() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();
        List<TopicPartition> partitions = List.of(
                new TopicPartition("orders", 4), new TopicPartition("orders", 3),
                new TopicPartition("orders", 2), new TopicPartition("orders", 1),
                new TopicPartition("orders", 0));
        Map<String, List<TopicPartition>> assigned = assignor.assign(partitions,
                List.of("g", "f", "e", "d", "c", "b", "a"));

        assertEquals(Map.of(
                "a", List.of(new TopicPartition("orders", 0)),
                "b", List.of(new TopicPartition("orders", 1)),
                "c", List.of(new TopicPartition("orders", 2)),
                "d", List.of(new TopicPartition("orders", 3)),
                "e", List.of(new TopicPartition("orders", 4)),
                "f", List.of(),
                "g", List.of()), assigned);
        assertEquals(Map.of(), assignor.assign(partitions, List.of()));
        assertEquals(Map.of("a", List.of(), "b", List.of()),
                assignor.assign(List.of(), List.of("b", "a")));
        assertEquals(Map.of(), assignor.assign(List.of(), List.of()));
    }

    @Test
    @DisplayName("Sorts cross topic partitions and member ids lexically after deduplication")
    void sortsCrossTopicPartitionsAndMemberIdsLexicallyAfterDeduplication() {
        RoundRobinAssignor assignor = new RoundRobinAssignor();
        List<TopicPartition> input = List.of(
                new TopicPartition("payments", 1), new TopicPartition("orders", 2),
                new TopicPartition("orders", 0), new TopicPartition("payments", 0),
                new TopicPartition("orders", 1), new TopicPartition("orders", 0));

        assertEquals(Map.of(
                "10", List.of(new TopicPartition("orders", 0)),
                "2", List.of(new TopicPartition("orders", 1)),
                "A", List.of(new TopicPartition("orders", 2)),
                "a", List.of(new TopicPartition("payments", 0)),
                "b", List.of(new TopicPartition("payments", 1))),
                assignor.assign(input, List.of("b", "10", "A", "a", "2", "a")));
        assertEquals(Map.of(
                "a", List.of(new TopicPartition("orders", 0), new TopicPartition("orders", 2),
                        new TopicPartition("payments", 1)),
                "b", List.of(new TopicPartition("orders", 1), new TopicPartition("payments", 0))),
                assignor.assign(input, List.of("b", "a", "a")));
    }
}
