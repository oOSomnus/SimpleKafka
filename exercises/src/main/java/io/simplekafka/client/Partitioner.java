package io.simplekafka.client;

import io.simplekafka.ExerciseNotImplementedException;

/** CRC32C keyed routing with per-instance round-robin routing for null keys. */
public final class Partitioner {
    /** Creates a partitioner instance. */
    public Partitioner() {}

    /**
     * Step 09: route non-null keys by unsigned CRC32C modulo the partition count and null keys by
     * instance-local round robin. An empty non-null key is still routed by its CRC32C value. Test:
     * Step09Test. Lesson: docs/book/chapters/03-partition-network.tex, Step 9.
     *
     * @param key nullable key bytes; null selects round-robin routing
     * @param partitions positive number of available partitions
     * @return the selected partition index
     * @throws io.simplekafka.CourseException with
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST} if {@code partitions} is nonpositive
     * @throws ExerciseNotImplementedException while the Step 9 exercise method is a skeleton
     */
    public int choose(byte[] key, int partitions) {
        throw new ExerciseNotImplementedException(9, "choose");
    }
}
