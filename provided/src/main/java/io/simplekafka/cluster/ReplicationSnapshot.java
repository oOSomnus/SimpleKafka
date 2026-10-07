package io.simplekafka.cluster;

import java.util.List;
import java.util.Map;

public record ReplicationSnapshot(int leaderId, int epoch, List<Integer> replicas, List<Integer> isr,
                                  long highWatermark, Map<Integer, Long> perReplicaLEO) {
    public ReplicationSnapshot {
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
        perReplicaLEO = Map.copyOf(perReplicaLEO);
    }
}
