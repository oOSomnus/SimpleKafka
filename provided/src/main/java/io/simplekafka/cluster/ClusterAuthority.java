package io.simplekafka.cluster;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.SystemTimeSource;
import io.simplekafka.support.TimeSource;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.locks.Condition;
import java.util.concurrent.locks.ReentrantLock;

/** Trusted, single-JVM teaching authority; it is deliberately not a replicated controller. */
public final class ClusterAuthority {
    private final TimeSource clock;
    private final Map<Integer, BrokerInfo> brokers = new TreeMap<>();
    private final Map<TopicPartition, PartitionState> partitions = new TreeMap<>();

    /** Creates an authority that uses the system wall clock. */
    public ClusterAuthority() {
        this(SystemTimeSource.INSTANCE);
    }

    /**
     * Creates an authority that uses the supplied clock for replication timing.
     *
     * @param clock source of millisecond timestamps used by authority state
     * @throws NullPointerException if {@code clock} is {@code null}
     */
    public ClusterAuthority(TimeSource clock) {
        this.clock = Objects.requireNonNull(clock);
    }

    /**
     * Registers a broker as online, or updates its endpoint if it is offline.
     *
     * @param brokerId broker identifier to register
     * @param endpoint network endpoint currently used by the broker
     * @throws NullPointerException if {@code endpoint} is {@code null}
     * @throws IllegalArgumentException if {@code brokerId} is negative or an online broker's
     *     endpoint would change
     */
    public synchronized void registerBroker(int brokerId, Endpoint endpoint) {
        Objects.requireNonNull(endpoint, "endpoint");
        if (brokerId < 0) throw new IllegalArgumentException("broker id must be nonnegative");
        BrokerInfo existing = brokers.get(brokerId);
        if (existing != null && existing.online && !existing.endpoint.equals(endpoint))
            throw new IllegalArgumentException("online broker endpoint changed");
        if (existing == null) brokers.put(brokerId, new BrokerInfo(endpoint, true));
        else {
            existing.endpoint = endpoint;
            existing.online = true;
        }
    }

    /**
     * Changes a registered broker's online state and signals partition state waiters.
     *
     * @param brokerId broker identifier to update
     * @param online whether the broker should be marked online
     * @throws CourseException with {@code UNKNOWN_MEMBER} if the broker is not registered
     */
    public void setBrokerOnline(int brokerId, boolean online) {
        List<PartitionState> affected;
        synchronized (this) {
            BrokerInfo info = brokers.get(brokerId);
            if (info == null)
                throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "unknown broker " + brokerId);
            info.online = online;
            affected = List.copyOf(partitions.values());
        }
        for (PartitionState state : affected) {
            state.lock.lock();
            try {
                state.changed.signalAll();
            } finally {
                state.lock.unlock();
            }
        }
    }

    /**
     * Reports whether a broker is online; an unknown broker is reported as offline.
     *
     * @param brokerId broker identifier to query
     * @return {@code true} only when the broker is registered and online
     */
    public synchronized boolean isOnline(int brokerId) {
        BrokerInfo info = brokers.get(brokerId);
        return info != null && info.online;
    }

    /**
     * Returns the registered endpoint for a broker, whether or not it is online.
     *
     * @param brokerId broker identifier to query
     * @return the endpoint last registered for the broker
     * @throws CourseException with {@code UNKNOWN_MEMBER} if the broker is not registered
     */
    public synchronized Endpoint endpoint(int brokerId) {
        BrokerInfo info = brokers.get(brokerId);
        if (info == null)
            throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "unknown broker " + brokerId);
        return info.endpoint;
    }

    /**
     * Returns broker identifiers in ascending order as an immutable snapshot.
     *
     * @return immutable list of registered broker identifiers
     */
    public synchronized List<Integer> brokerIds() {
        return List.copyOf(brokers.keySet());
    }

    /**
     * Creates partition authority state with all configured replicas initially in the ISR.
     *
     * @param tp partition to create
     * @param leaderId broker selected as the initial leader
     * @param replicaIds broker identifiers assigned to the partition
     * @param minISR minimum number of in-sync replicas required for writes
     * @return the newly created mutable partition state
     * @throws NullPointerException if {@code tp}, {@code replicaIds}, or an element of {@code
     *     replicaIds} is {@code null}
     * @throws IllegalArgumentException if the partition exists, replica identifiers are empty or
     *     duplicated, the leader is not a replica, {@code minISR} is out of range, or a replica is
     *     not registered
     */
    public synchronized PartitionState createPartition(
            TopicPartition tp, int leaderId, Collection<Integer> replicaIds, int minISR) {
        Objects.requireNonNull(tp, "tp");
        Objects.requireNonNull(replicaIds, "replicaIds");
        if (partitions.containsKey(tp))
            throw new IllegalArgumentException("partition already exists: " + tp);
        TreeSet<Integer> replicas = new TreeSet<>(replicaIds);
        if (replicas.isEmpty()
                || replicas.size() != replicaIds.size()
                || !replicas.contains(leaderId))
            throw new IllegalArgumentException("replicas must be unique and include leader");
        if (minISR < 1 || minISR > replicas.size())
            throw new IllegalArgumentException("invalid minISR");
        for (int id : replicas)
            if (!brokers.containsKey(id))
                throw new IllegalArgumentException("unregistered broker " + id);
        PartitionState state =
                new PartitionState(tp, leaderId, replicas, minISR, clock.nowMillis());
        partitions.put(tp, state);
        return state;
    }

    /**
     * Returns mutable authority state for a partition.
     *
     * @param tp partition to look up
     * @return the partition state
     * @throws CourseException with {@code UNKNOWN_TOPIC_OR_PARTITION} if the partition is absent
     */
    public synchronized PartitionState partitionState(TopicPartition tp) {
        PartitionState state = partitions.get(tp);
        if (state == null)
            throw new CourseException(
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown partition " + tp);
        return state;
    }

    /**
     * Returns metadata for one partition while holding that partition's state lock.
     *
     * @param tp partition to describe
     * @return current leader and replica metadata
     * @throws CourseException with {@code UNKNOWN_TOPIC_OR_PARTITION} if the partition is absent
     */
    public PartitionMetadata metadata(TopicPartition tp) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            return state.metadata(endpoint(state.leaderId));
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Returns metadata for a topic's partitions in ascending partition order.
     *
     * <p>Each partition is read separately, so the result is not an atomic snapshot across the
     * topic.
     *
     * @param topic topic name to look up
     * @return immutable list of partition metadata
     * @throws CourseException with {@code UNKNOWN_TOPIC_OR_PARTITION} if the topic has no
     *     partitions
     */
    public List<PartitionMetadata> metadata(String topic) {
        List<TopicPartition> keys;
        synchronized (this) {
            keys = partitions.keySet().stream().filter(tp -> tp.topic().equals(topic)).toList();
        }
        if (keys.isEmpty())
            throw new CourseException(
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic " + topic);
        List<PartitionMetadata> result = new ArrayList<>(keys.size());
        for (TopicPartition tp : keys) result.add(metadata(tp));
        return List.copyOf(result);
    }

    /**
     * Returns a consistent snapshot of one partition's replication state.
     *
     * @param tp partition to snapshot
     * @return replication state copied while holding the partition lock
     * @throws CourseException with {@code UNKNOWN_TOPIC_OR_PARTITION} if the partition is absent
     */
    public ReplicationSnapshot snapshot(TopicPartition tp) {
        PartitionState state = partitionState(tp);
        state.lock.lock();
        try {
            return state.snapshot();
        } finally {
            state.lock.unlock();
        }
    }

    /**
     * Returns the clock used for broker liveness and replication timing.
     *
     * @return the injected clock
     */
    public TimeSource clock() {
        return clock;
    }

    private static final class BrokerInfo {
        private Endpoint endpoint;
        private volatile boolean online;

        private BrokerInfo(Endpoint endpoint, boolean online) {
            this.endpoint = endpoint;
            this.online = online;
        }
    }

    /**
     * Mutable only while lock is held.
     *
     * <p>Lock order: authority state ({@code lock}), then local partition log monitor and its
     * internal lock.
     *
     * <p>Callers must hold {@link #lock} when reading or changing mutable state. The methods on
     * this type do not acquire that lock themselves.
     */
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

        private PartitionState(
                TopicPartition tp, int leaderId, Set<Integer> replicas, int minISR, long now) {
            this.tp = tp;
            this.leaderId = leaderId;
            this.minISR = minISR;
            this.replicas.addAll(replicas);
            this.isr.addAll(replicas);
            for (int id : replicas) {
                reportedLEO.put(id, 0L);
                lastCaughtUpMillis.put(id, now);
            }
        }

        /**
         * Tests whether a broker is assigned to this partition.
         *
         * <p>The caller must hold {@link #lock}; this method does not acquire it.
         *
         * @param brokerId broker identifier to test
         * @return {@code true} if the broker is in {@code replicas}
         */
        public boolean assigned(int brokerId) {
            return replicas.contains(brokerId);
        }

        /**
         * Builds metadata from this state and the caller-supplied leader endpoint.
         *
         * <p>The caller must hold {@link #lock}; this method does not acquire it.
         *
         * @param endpoint endpoint for the current leader
         * @return metadata with copied replica and ISR lists
         * @throws NullPointerException if {@code endpoint} is {@code null}
         */
        public PartitionMetadata metadata(Endpoint endpoint) {
            return new PartitionMetadata(
                    tp, leaderId, epoch, List.copyOf(replicas), List.copyOf(isr), endpoint);
        }

        /**
         * Copies the current replication state into a snapshot.
         *
         * <p>The caller must hold {@link #lock}; this method does not acquire it.
         *
         * @return snapshot with copied replica, ISR, and per-replica LEO collections
         */
        public ReplicationSnapshot snapshot() {
            return new ReplicationSnapshot(
                    leaderId,
                    epoch,
                    List.copyOf(replicas),
                    List.copyOf(isr),
                    highWatermark,
                    new TreeMap<>(reportedLEO));
        }
    }
}
