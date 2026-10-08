package io.simplekafka.group;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.TopicPartition;

import java.util.List;
import java.util.Map;

/** Deterministic round-robin assignment of topic partitions to consumer-group members. */
public final class RoundRobinAssignor {
    /** Creates a stateless assignor. */
    public RoundRobinAssignor() {}

    /**
     * Step 17: sort and deduplicate the inputs, then distribute each partition to the next sorted
     * member. Every member appears even when it receives no partitions; no members yields an empty
     * map. Test: Step17Test. Lesson: docs/book/chapters/05-consumer-groups.tex, Step 17.
     *
     * @param partitions topic partitions to distribute
     * @param members consumer member identifiers
     * @return an unmodifiable member-to-partitions map with unmodifiable partition lists
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     *     for null inputs or elements, or a member identifier outside the 1–255 UTF-8 byte range
     * @throws ExerciseNotImplementedException while the Step 17 exercise method is a skeleton
     */
    public Map<String, List<TopicPartition>> assign(
            List<TopicPartition> partitions, List<String> members) {
        throw new ExerciseNotImplementedException(17, "assign");
    }
}
