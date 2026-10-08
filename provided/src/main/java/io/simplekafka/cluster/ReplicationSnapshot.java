package io.simplekafka.cluster;

import java.util.List;
import java.util.Map;

/**
 * Snapshot of replica leadership, membership, and replication progress.
 *
 * <p>Collection components are copied into immutable collections. The lists preserve their input
 * order; authority snapshots provide replica and ISR lists in ascending broker order. The
 * per-replica map does not guarantee an iteration order.
 *
 * @param leaderId broker identifier currently recorded as leader
 * @param epoch leader epoch in the snapshot
 * @param replicas assigned broker identifiers
 * @param isr in-sync broker identifiers
 * @param highWatermark highest committed offset boundary
 * @param perReplicaLEO reported log-end offset by broker identifier
 */
public record ReplicationSnapshot(
        int leaderId,
        int epoch,
        List<Integer> replicas,
        List<Integer> isr,
        long highWatermark,
        Map<Integer, Long> perReplicaLEO) {
    /**
     * Copies the collection components into immutable collections.
     *
     * @param leaderId broker identifier recorded as leader
     * @param epoch leader epoch in the snapshot
     * @param replicas assigned broker identifiers
     * @param isr in-sync broker identifiers
     * @param highWatermark highest committed offset boundary
     * @param perReplicaLEO reported log-end offset by broker identifier
     * @throws NullPointerException if {@code replicas}, {@code isr}, or {@code perReplicaLEO} is
     *     {@code null}, any list element is {@code null}, or a map key or value is {@code null}
     */
    public ReplicationSnapshot {
        replicas = List.copyOf(replicas);
        isr = List.copyOf(isr);
        perReplicaLEO = Map.copyOf(perReplicaLEO);
    }
}
