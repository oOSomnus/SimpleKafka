package io.simplekafka.cluster;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.TimeSource;
import io.simplekafka.support.SystemTimeSource;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.Objects;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Trusted, single-JVM teaching authority; it is deliberately not a replicated controller. */
public final class ClusterAuthority {
    private final TimeSource clock;
    private final Map<Integer, BrokerInfo> brokers = new TreeMap<>();
    private final Map<TopicPartition, PartitionState> partitions = new TreeMap<>();

    public ClusterAuthority() { this(SystemTimeSource.INSTANCE); }
    public ClusterAuthority(TimeSource clock) { this.clock = Objects.requireNonNull(clock); }

    public synchronized void registerBroker(int brokerId, Endpoint endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        if (brokerId < 0) throw new IllegalArgumentException("broker id must be nonnegative");
        BrokerInfo existing = brokers.get(brokerId);
        if (existing != null && existing.online && !existing.endpoint.equals(endpoint))
            throw new IllegalArgumentException("online broker endpoint changed");
        if (existing == null) brokers.put(brokerId, new BrokerInfo(endpoint, true));
        else { existing.endpoint = endpoint; existing.online = true; }
    }
    public void setBrokerOnline(int brokerId, boolean online) {
        List<PartitionState> affected;
        synchronized (this) {
            BrokerInfo info = brokers.get(brokerId);
            if (info == null) throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "unknown broker " + brokerId);
            info.online = online;
            affected = List.copyOf(partitions.values());
        }
        for (PartitionState state : affected) {
            state.lock.lock();
            try { state.changed.signalAll(); } finally { state.lock.unlock(); }
        }
    }
    public synchronized boolean isOnline(int brokerId) {
        BrokerInfo info = brokers.get(brokerId);
        return info != null && info.online;
    }
    public synchronized Endpoint endpoint(int brokerId) {
        BrokerInfo info = brokers.get(brokerId);
        if (info == null) throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "unknown broker " + brokerId);
        return info.endpoint;
    }
    public synchronized List<Integer> brokerIds() { return List.copyOf(brokers.keySet()); }

    public synchronized PartitionState createPartition(TopicPartition tp, int leaderId,
                                                       Collection<Integer> replicaIds, int minISR) {
        Objects.requireNonNull(tp, "tp");
        Objects.requireNonNull(replicaIds, "replicaIds");
        if (partitions.containsKey(tp)) throw new IllegalArgumentException("partition already exists: " + tp);
        TreeSet<Integer> replicas = new TreeSet<>(replicaIds);
        if (replicas.isEmpty() || replicas.size() != replicaIds.size() || !replicas.contains(leaderId))
            throw new IllegalArgumentException("replicas must be unique and include leader");
        if (minISR < 1 || minISR > replicas.size()) throw new IllegalArgumentException("invalid minISR");
        for (int id : replicas) if (!brokers.containsKey(id)) throw new IllegalArgumentException("unregistered broker " + id);
        PartitionState state = new PartitionState(tp, leaderId, replicas, minISR, clock.nowMillis());
        partitions.put(tp, state);
        return state;
    }
    public synchronized PartitionState partitionState(TopicPartition tp) {
        PartitionState state = partitions.get(tp);
        if (state == null) throw new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown partition " + tp);
        return state;
    }
    public PartitionMetadata metadata(TopicPartition tp) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try { return state.metadata(endpoint(state.leaderId)); } finally { state.lock.unlock(); }
    }
    public List<PartitionMetadata> metadata(String topic) {
        List<TopicPartition> keys;
        synchronized (this) { keys = partitions.keySet().stream().filter(tp -> tp.topic().equals(topic)).toList(); }
        if (keys.isEmpty()) throw new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic " + topic);
        List<PartitionMetadata> result = new ArrayList<>(keys.size());
        for (TopicPartition tp : keys) result.add(metadata(tp));
        return List.copyOf(result);
    }
    public ReplicationSnapshot snapshot(TopicPartition tp) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try { return state.snapshot(); } finally { state.lock.unlock(); }
    }
    public TimeSource clock() { return clock; }

    private static final class BrokerInfo {
        private Endpoint endpoint;
        private volatile boolean online;
        private BrokerInfo(Endpoint endpoint, boolean online) { this.endpoint = endpoint; this.online = online; }
    }

    /** Mutable only while lock is held. Lock order: authority state, then local partition log. */
    public static final class PartitionState {
        public final TopicPartition tp;
        public final ReentrantLock lock = new ReentrantLock();
        public final Condition changed = lock.newCondition();
        public final Set<Integer> replicas = new TreeSet<>();
        public final Set<Integer> isr = new TreeSet<>();
        public final Map<Integer, Long> reportedLEO = new TreeMap<>();
        public final Map<Integer, Long> lastCaughtUpMillis = new TreeMap<>();
        public int leaderId;
        public int epoch;
        public final int minISR;
        public long highWatermark;

        private PartitionState(TopicPartition tp, int leaderId, Set<Integer> replicas, int minISR, long now) {
            this.tp = tp; this.leaderId = leaderId; this.minISR = minISR;
            this.replicas.addAll(replicas); this.isr.addAll(replicas);
            for (int id : replicas) { reportedLEO.put(id, 0L); lastCaughtUpMillis.put(id, now); }
        }
        public boolean assigned(int brokerId) { return replicas.contains(brokerId); }
        public PartitionMetadata metadata(Endpoint endpoint) {
            return new PartitionMetadata(tp, leaderId, epoch, List.copyOf(replicas), List.copyOf(isr), endpoint);
        }
        public ReplicationSnapshot snapshot() {
            return new ReplicationSnapshot(leaderId, epoch, List.copyOf(replicas), List.copyOf(isr), highWatermark, new TreeMap<>(reportedLEO));
        }
    }
}
