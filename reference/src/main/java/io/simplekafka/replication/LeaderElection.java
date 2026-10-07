package io.simplekafka.replication;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.cluster.ClusterAuthority.PartitionState;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import java.util.Set;
import java.util.Objects;

/** Controlled clean election over the authority's previous ISR only. */
public final class LeaderElection {
    private final ClusterAuthority authority;

    public LeaderElection(ClusterAuthority authority) { this.authority = Objects.requireNonNull(authority); }

    public PartitionMetadata elect(TopicPartition tp) {
        PartitionState state = authority.partitionState(tp);
        while (true) {
            int observedLeader;
            int observedEpoch;
            Set<Integer> observedIsr;
            state.lock.lock();
            try {
                observedLeader = state.leaderId;
                observedEpoch = state.epoch;
                observedIsr = new java.util.TreeSet<>(state.isr);
            } finally {
                state.lock.unlock();
            }

            boolean leaderOnline = authority.isOnline(observedLeader);
            Set<Integer> onlineCandidates = new java.util.HashSet<>();
            if (!leaderOnline) {
                for (int candidate : observedIsr)
                    if (authority.isOnline(candidate)) onlineCandidates.add(candidate);
            }
            boolean retry = false;
            state.lock.lock();
            try {
                if (state.epoch != observedEpoch || state.leaderId != observedLeader) {
                    retry = true;
                } else if (!leaderOnline) {
                    int selected = -1;
                    long selectedLeo = -1;
                    for (int candidate : state.isr) {
                        if (!onlineCandidates.contains(candidate)) continue;
                        long leo = state.reportedLEO.getOrDefault(candidate, -1L);
                        if (leo < state.highWatermark) continue;
                        if (selected < 0 || candidate < selected) {
                            selected = candidate;
                            selectedLeo = leo;
                        }
                    }
                    if (selected < 0)
                        throw new CourseException(ErrorCode.NO_ELIGIBLE_LEADER,
                                "no online ISR replica contains the committed prefix");

                    state.epoch++;
                    state.leaderId = selected;
                    state.isr.clear();
                    state.isr.add(selected);
                    for (int brokerId : state.replicas) {
                        state.reportedLEO.put(brokerId, brokerId == selected ? selectedLeo : 0L);
                        state.lastCaughtUpMillis.put(brokerId,
                                brokerId == selected ? authority.clock().nowMillis() : Long.MIN_VALUE);
                    }
                    state.changed.signalAll();
                }
            } finally {
                state.lock.unlock();
            }
            if (retry) continue;
            return authority.metadata(tp);
        }
    }

    public void checkLeader(int brokerId, TopicPartition tp, int epoch) {
        if (brokerId < 0 || epoch < 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "negative broker id or epoch");
        PartitionState state = authority.partitionState(tp);
        boolean online = authority.isOnline(brokerId);
        state.lock.lock();
        try {
            if (epoch != state.epoch) throw new CourseException(ErrorCode.FENCED_EPOCH, "request epoch is stale");
            if (brokerId != state.leaderId || !online)
                throw new CourseException(ErrorCode.NOT_LEADER, "broker is not the online partition leader");
        } finally {
            state.lock.unlock();
        }
    }
}
