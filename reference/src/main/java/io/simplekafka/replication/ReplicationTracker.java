package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.cluster.ReplicationSnapshot;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.TimeSource;
import java.util.HashSet;
import java.util.Objects;
import java.util.Set;

/** Serializes replica progress, ISR membership, and the committed prefix per partition. */
public final class ReplicationTracker {
    private final ClusterAuthority authority;
    private final TopicPartition tp;
    private final int minISR;
    private final TimeSource clock;
    private final long lagTimeoutMillis;
    private final PartitionState state;

    public ReplicationTracker(ClusterAuthority authority, TopicPartition tp, int leaderId,
                              Set<Integer> replicas, int minISR, TimeSource clock, long lagTimeoutMillis) {
        this.authority = Objects.requireNonNull(authority);
        this.tp = Objects.requireNonNull(tp);
        Set<Integer> configuredReplicas = Set.copyOf(Objects.requireNonNull(replicas));
        this.minISR = minISR;
        this.clock = Objects.requireNonNull(clock);
        if (lagTimeoutMillis < 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "negative lag timeout");
        this.lagTimeoutMillis = lagTimeoutMillis;
        this.state = authority.partitionState(tp);
        if (minISR < 1 || minISR != state.minISR || !configuredReplicas.equals(state.replicas)
                || leaderId != state.leaderId)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "tracker configuration differs from partition authority");
    }

    public void report(int brokerId, int epoch, long leo) {
        if (brokerId < 0 || epoch < 0 || leo < 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "negative replica report field");
        boolean online = state.lock.isHeldByCurrentThread() || authority.isOnline(brokerId);
        if (!online) throw new CourseException(ErrorCode.INVALID_REQUEST, "offline broker cannot report replica progress");
        state.lock.lock();
        try {
            if (epoch != state.epoch) throw new CourseException(ErrorCode.FENCED_EPOCH, "replica report has a stale epoch");
            if (!state.assigned(brokerId)) throw new CourseException(ErrorCode.INVALID_REQUEST, "broker is not assigned to partition");
            Long oldValue = state.reportedLEO.get(brokerId);
            if (oldValue == null) throw new CourseException(ErrorCode.INVALID_REQUEST, "assigned replica has no progress slot");
            long leaderLeo = state.reportedLEO.getOrDefault(state.leaderId, 0L);
            if (brokerId == state.leaderId) {
                if (leo < oldValue) throw new CourseException(ErrorCode.INVALID_REQUEST, "leader LEO cannot move backwards");
                leaderLeo = leo;
            } else {
                if (leo > leaderLeo) throw new CourseException(ErrorCode.INVALID_REQUEST, "follower LEO exceeds leader LEO");
                if (leo < oldValue) throw new CourseException(ErrorCode.INVALID_REQUEST, "follower LEO cannot move backwards in one epoch");
            }
            state.reportedLEO.put(brokerId, leo);
            if (leo == leaderLeo) state.lastCaughtUpMillis.put(brokerId, clock.nowMillis());
            advanceHighWatermarkLocked();
            state.changed.signalAll();
        } finally {
            state.lock.unlock();
        }
    }

    public Set<Integer> expireLagging() {
        long now = clock.nowMillis();
        state.lock.lock();
        try {
            Set<Integer> expired = new HashSet<>();
            for (int brokerId : new HashSet<>(state.isr)) {
                if (brokerId == state.leaderId) continue;
                Long lastCaughtUp = state.lastCaughtUpMillis.get(brokerId);
                if (lastCaughtUp == null || elapsedAtLeast(now, lastCaughtUp, lagTimeoutMillis)) {
                    state.isr.remove(brokerId);
                    expired.add(brokerId);
                }
            }
            if (!expired.isEmpty()) {
                advanceHighWatermarkLocked();
                state.changed.signalAll();
            }
            return Set.copyOf(expired);
        } finally {
            state.lock.unlock();
        }
    }

    public long advanceHighWatermark() {
        state.lock.lock();
        try {
            advanceHighWatermarkLocked();
            return state.highWatermark;
        } finally {
            state.lock.unlock();
        }
    }

    private long advanceHighWatermarkLocked() {
        if (state.isr.size() < minISR) return state.highWatermark;
        long candidate = Long.MAX_VALUE;
        for (int brokerId : state.isr) {
            Long leo = state.reportedLEO.get(brokerId);
            if (leo == null) return state.highWatermark;
            candidate = Math.min(candidate, leo);
        }
        if (candidate > state.highWatermark) {
            state.highWatermark = candidate;
            state.changed.signalAll();
        }
        return state.highWatermark;
    }

    private static boolean elapsedAtLeast(long now, long then, long timeout) {
        if (now < then) return false;
        if (timeout == 0) return true;
        if (then > Long.MAX_VALUE - timeout) return false;
        return now >= then + timeout;
    }

    ClusterAuthority authority() { return authority; }
    TopicPartition tp() { return tp; }
    PartitionState state() { return state; }
    public int minISR() { return minISR; }
    public int epoch() { state.lock.lock(); try { return state.epoch; } finally { state.lock.unlock(); } }
    public int leaderId() { state.lock.lock(); try { return state.leaderId; } finally { state.lock.unlock(); } }
    public long highWatermark() { state.lock.lock(); try { return state.highWatermark; } finally { state.lock.unlock(); } }
    public ReplicationSnapshot snapshot() { return authority.snapshot(tp); }
}
