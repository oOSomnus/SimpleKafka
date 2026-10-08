package io.simplekafka.client;

import io.simplekafka.ExerciseNotImplementedException;

/** CRC32C keyed routing with per-instance round-robin routing for null keys. */
public final class Partitioner {
    public Partitioner() {}

    /**
     * Step 09: implement CRC32C routing for non-null keys and instance-local round robin for null
     * keys. Contract: reject nonpositive partition counts; an empty key is still keyed. Test:
     * Step09Test. Lesson: docs/book/chapters/03-partition-network.tex, Step 9.
     */
    public int choose(byte[] key, int partitions) {
        throw new ExerciseNotImplementedException(9, "choose");
    }
}
