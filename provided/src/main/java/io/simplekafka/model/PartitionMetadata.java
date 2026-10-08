package io.simplekafka.model;

import java.util.List;
import java.util.Objects;

/**
 * Metadata describing a partition's leader and replica membership.
 *
 * @param tp topic-partition described by this metadata
 * @param leaderId broker identifier of the leader
 * @param epoch current leader epoch
 * @param replicas replica broker identifiers; copied into an immutable list
 * @param isr in-sync replica broker identifiers, a subset of {@code replicas} by contract; copied
 *     into an immutable list
 * @param leader endpoint for the broker identified by {@code leaderId}
 */
public record PartitionMetadata(
        TopicPartition tp,
        int leaderId,
        int epoch,
        List<Integer> replicas,
        List<Integer> isr,
        Endpoint leader) {
    /**
     * Creates partition metadata and copies the replica lists.
     *
     * @param tp topic-partition described by this metadata
     * @param leaderId broker identifier of the leader
     * @param epoch current leader epoch
     * @param replicas replica broker identifiers
     * @param isr in-sync replica broker identifiers
     * @param leader endpoint for the broker identified by {@code leaderId}
     * @throws NullPointerException if a required value, replica list, or list element is null
     * @throws IllegalArgumentException if {@code leaderId} or {@code epoch} is negative
     */
    public PartitionMetadata {
        Objects.requireNonNull(tp, "tp");
        if (leaderId < 0 || epoch < 0)
            throw new IllegalArgumentException("leaderId and epoch must be nonnegative");
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
        Objects.requireNonNull(leader, "leader");
    }
}
