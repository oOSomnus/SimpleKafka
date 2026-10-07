package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.RecoveryProof;
import io.simplekafka.cluster.RecoveryProofRegistry;
import io.simplekafka.model.TopicPartition;
import java.util.Objects;

/** Adds a recovered replica only while its proof still describes the current leader prefix. */
public final class ReplicaAdmission {
    private final ClusterAuthority authority;

    public ReplicaAdmission(ClusterAuthority authority) { this.authority = Objects.requireNonNull(authority); }

    public boolean tryAdd(int brokerId, TopicPartition tp, int epoch) {
        if (brokerId < 0 || epoch < 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "negative broker id or epoch");
        PartitionState state = authority.partitionState(tp);
        boolean online = authority.isOnline(brokerId);
        state.lock.lock();
        try {
            if (!state.assigned(brokerId)) throw new CourseException(ErrorCode.INVALID_REQUEST, "broker is not assigned to partition");
            if (epoch != state.epoch || !online || brokerId == state.leaderId) return false;
            if (state.isr.contains(brokerId)) return true;
            RecoveryProof proof = RecoveryProofRegistry.find(authority, tp, brokerId).orElse(null);
            long leaderLeo = state.reportedLEO.getOrDefault(state.leaderId, -1L);
            if (proof == null || proof.brokerId() != brokerId || !proof.tp().equals(tp)
                    || proof.epoch() != state.epoch || proof.leaderLEO() != leaderLeo
                    || proof.localLEO() != leaderLeo || proof.highWatermark() > proof.leaderLEO()) {
                RecoveryProofRegistry.clear(authority, tp, brokerId);
                return false;
            }
            state.isr.add(brokerId);
            state.reportedLEO.put(brokerId, leaderLeo);
            state.lastCaughtUpMillis.put(brokerId, authority.clock().nowMillis());
            advanceHighWatermark(state);
            state.changed.signalAll();
            RecoveryProofRegistry.clear(authority, tp, brokerId);
            return true;
        } finally {
            state.lock.unlock();
        }
    }

    private static void advanceHighWatermark(PartitionState state) {
        if (state.isr.size() < state.minISR) return;
        long candidate = Long.MAX_VALUE;
        for (int member : state.isr) {
            Long leo = state.reportedLEO.get(member);
            if (leo == null) return;
            candidate = Math.min(candidate, leo);
        }
        if (candidate > state.highWatermark) state.highWatermark = candidate;
    }
}
