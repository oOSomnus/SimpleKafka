package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import java.util.zip.CRC32C;

/** CRC32C keyed routing with per-instance round-robin routing for null keys. */
public final class Partitioner {
    private int nextNullPartition;

    public Partitioner() {}

    public int choose(byte[] key, int partitions) {
        if (partitions <= 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "partitions must be positive");
        if (key == null) {
            int chosen = nextNullPartition % partitions;
            nextNullPartition = (chosen + 1) % partitions;
            return chosen;
        }
        CRC32C crc = new CRC32C();
        crc.update(key, 0, key.length);
        return (int) (crc.getValue() % partitions);
    }
}
