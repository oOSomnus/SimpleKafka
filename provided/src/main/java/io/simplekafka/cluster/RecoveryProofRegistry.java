package io.simplekafka.cluster;

import io.simplekafka.model.TopicPartition;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.WeakHashMap;

/** In-JVM storage for recovery evidence; evidence is scoped to an authority and replica. */
public final class RecoveryProofRegistry {
    private static final Map<ClusterAuthority, Map<Key, RecoveryProof>> PROOFS =
            new WeakHashMap<>();

    private RecoveryProofRegistry() {}

    /**
     * Removes the stored recovery proof for an authority, partition, and broker.
     *
     * @param authority authority whose proof entry is cleared
     * @param tp partition associated with the proof
     * @param brokerId broker associated with the proof
     */
    public static synchronized void clear(
            ClusterAuthority authority, TopicPartition tp, int brokerId) {
        Map<Key, RecoveryProof> proofs = PROOFS.get(authority);
        if (proofs != null) proofs.remove(new Key(tp, brokerId));
    }

    /**
     * Stores or replaces a proof for its authority, partition, and broker.
     *
     * @param authority authority that scopes the proof
     * @param proof recovery proof to store
     */
    public static synchronized void record(ClusterAuthority authority, RecoveryProof proof) {
        PROOFS.computeIfAbsent(authority, ignored -> new HashMap<>())
                .put(new Key(proof.tp(), proof.brokerId()), proof);
    }

    /**
     * Looks up a proof in the authority-scoped registry.
     *
     * @param authority authority that scopes the proof
     * @param tp partition associated with the proof
     * @param brokerId broker associated with the proof
     * @return the stored proof, or {@link Optional#empty()} when none is present
     */
    public static synchronized Optional<RecoveryProof> find(
            ClusterAuthority authority, TopicPartition tp, int brokerId) {
        Map<Key, RecoveryProof> proofs = PROOFS.get(authority);
        return proofs == null
                ? Optional.empty()
                : Optional.ofNullable(proofs.get(new Key(tp, brokerId)));
    }

    private record Key(TopicPartition tp, int brokerId) {}
}
