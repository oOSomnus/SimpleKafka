package io.simplekafka.group;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.TopicPartition;
import java.util.List;
import java.util.Map;

public final class RoundRobinAssignor {
    public RoundRobinAssignor() {}

    /**
     * Step 17: sort/deduplicate input and distribute each partition to the next sorted member.
     * Contract: every member appears (even with no partitions); empty members yields an empty map.
     * Test: Step17Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 17.
     */
    public Map<String, List<TopicPartition>> assign(List<TopicPartition> partitions, List<String> members) {
        throw new ExerciseNotImplementedException(17, "assign");
    }
}
