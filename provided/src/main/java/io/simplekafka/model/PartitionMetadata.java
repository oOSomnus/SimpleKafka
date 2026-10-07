package io.simplekafka.model;

import java.util.List;
import java.util.Objects;

public record PartitionMetadata(TopicPartition tp, int leaderId, int epoch, List<Integer> replicas,
                                List<Integer> isr, Endpoint leader) {
    public PartitionMetadata {
        Objects.requireNonNull(tp, "tp");
        if (leaderId < 0 || epoch < 0) throw new IllegalArgumentException("leaderId and epoch must be nonnegative");
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
        Objects.requireNonNull(leader, "leader");
    }
}
