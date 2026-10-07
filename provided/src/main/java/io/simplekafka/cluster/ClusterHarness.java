package io.simplekafka.cluster;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.replication.AckPolicy;
import io.simplekafka.replication.FollowerReplicator;
import io.simplekafka.replication.LeaderElection;
import io.simplekafka.replication.ReplicaAdmission;
import io.simplekafka.replication.ReplicaReconciler;
import io.simplekafka.replication.ReplicatedPartition;
import io.simplekafka.replication.ReplicationTracker;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.SystemTimeSource;
import io.simplekafka.support.TimeSource;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.atomic.AtomicReference;

/** Three loopback TCP brokers sharing one intentionally single-JVM cluster authority. */
public final class ClusterHarness implements AutoCloseable {
    public static final List<Integer> BROKER_IDS = List.of(1, 2, 3);
    private static final long DEFAULT_LAG_TIMEOUT_MILLIS = 1_000;
    private static final long DEFAULT_GROUP_TIMEOUT_MILLIS = 10_000;
    private static final int DEFAULT_RPC_TIMEOUT_MILLIS = 5_000;

    private final Path root;
    private final TimeSource clock;
    private final ClusterAuthority authority;
    private final Map<Integer, BrokerNode> nodes = new TreeMap<>();
    private final Map<TopicPartition, ReplicationTracker> trackers = new TreeMap<>();
    private final Map<String, TopicSpec> topics = new TreeMap<>();
    private OffsetStore offsets;
    private GroupCoordinator groups;
    private boolean started;
    private boolean closed;

    public ClusterHarness(Path root) { this(root, SystemTimeSource.INSTANCE); }

    public ClusterHarness(Path root, TimeSource clock) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        this.clock = Objects.requireNonNull(clock, "clock");
        this.authority = new ClusterAuthority(clock);
        try {
            Files.createDirectories(this.root);
            this.offsets = new OffsetStore(this.root.resolve("group-offsets"));
        } catch (IOException exception) {
            throw new CourseException(ErrorCode.STORAGE_ERROR, "cannot create cluster harness root", exception);
        }
    }

    /** Starts three independent loopback servers, each bound to an OS-assigned port. */
    public synchronized void start() {
        ensureNotClosed();
        if (started) return;
        try {
            for (int brokerId : BROKER_IDS) {
                AtomicReference<BrokerHandler> handler = new AtomicReference<>();
                BrokerServer server = new BrokerServer("127.0.0.1", 0, request -> {
                    BrokerHandler current = handler.get();
                    return current == null
                            ? io.simplekafka.protocol.Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "broker initialization is incomplete")
                            : current.handle(request);
                });
                Endpoint endpoint = server.start();
                try {
                    authority.registerBroker(brokerId, endpoint);
                    Path brokerRoot = root.resolve("broker-" + brokerId);
                    PartitionCatalog catalog = new PartitionCatalog(brokerRoot, brokerId, endpoint);
                    nodes.put(brokerId, new BrokerNode(brokerId, server, endpoint, catalog, handler));
                } catch (RuntimeException failure) {
                    server.close();
                    throw failure;
                }
            }
            PartitionCatalog coordinatorCatalog = nodes.get(2).catalog;
            groups = new GroupCoordinator(coordinatorCatalog, new RoundRobinAssignor(), clock,
                    DEFAULT_GROUP_TIMEOUT_MILLIS);
            for (BrokerNode node : nodes.values())
                node.handler.set(new BrokerHandler(node.catalog, authority, groups, offsets));
            started = true;
        } catch (RuntimeException failure) {
            closeAfterFailedStart(failure);
            throw failure;
        }
    }

    /** Creates fixed RF3 partitions: leaders rotate 1,2,3 and every replica starts in the ISR. */
    public synchronized void createTopic(String topic, int partitions, int minISR) {
        requireStarted();
        if (topic == null || partitions <= 0 || minISR < 1 || minISR > BROKER_IDS.size())
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid replicated topic configuration");
        TopicSpec existing = topics.get(topic);
        TopicSpec requested = new TopicSpec(partitions, minISR);
        if (existing != null) {
            if (!existing.equals(requested)) throw new CourseException(ErrorCode.INVALID_REQUEST, "topic configuration changed");
            return;
        }
        for (BrokerNode node : nodes.values()) {
            if (!authority.isOnline(node.brokerId))
                throw new CourseException(ErrorCode.NOT_LEADER, "all brokers must be online when creating a replicated topic");
            node.catalog.createTopic(topic, partitions);
        }
        List<Integer> replicaIds = List.copyOf(BROKER_IDS);
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition tp = new TopicPartition(topic, partition);
            int leaderId = BROKER_IDS.get(partition % BROKER_IDS.size());
            authority.createPartition(tp, leaderId, replicaIds, minISR);
            ReplicationTracker tracker = new ReplicationTracker(authority, tp, leaderId,
                    Set.copyOf(replicaIds), minISR, clock, DEFAULT_LAG_TIMEOUT_MILLIS);
            AckPolicy policy = new AckPolicy(tracker);
            trackers.put(tp, tracker);
            for (BrokerNode node : nodes.values()) {
                ReplicaState replica = new ReplicaState(node.brokerId, 0, node.brokerId == leaderId, true);
                ReplicatedPartition backend = new ReplicatedPartition(tp, node.catalog.partition(tp),
                        replica, tracker, policy);
                node.replicaStates.put(tp, replica);
                node.catalog.installBackend(tp, backend);
            }
        }
        topics.put(topic, requested);
    }

    public synchronized Endpoint endpoint(int brokerId) { return node(brokerId).endpoint; }
    public synchronized PartitionCatalog catalog(int brokerId) { return node(brokerId).catalog; }
    public ClusterAuthority authority() { return authority; }

    /** Stops only this broker's TCP server; authority, catalog, log handles, and peers stay alive. */
    public synchronized void stopBroker(int brokerId) {
        requireStarted();
        BrokerNode node = node(brokerId);
        if (node.server == null) return;
        authority.setBrokerOnline(brokerId, false);
        for (ReplicaState replica : node.replicaStates.values())
            replica.update(replica.epoch(), replica.isLeader(), false);
        node.server.close();
        node.server = null;
    }

    /** Restarts the broker socket on a fresh ephemeral port and preserves its independent disk/catalog. */
    public synchronized void restartBroker(int brokerId) {
        requireStarted();
        BrokerNode node = node(brokerId);
        if (node.server != null) return;
        AtomicReference<BrokerHandler> handler = node.handler;
        BrokerServer server = new BrokerServer("127.0.0.1", 0, request -> {
            BrokerHandler current = handler.get();
            return current == null
                    ? io.simplekafka.protocol.Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "broker initialization is incomplete")
                    : current.handle(request);
        });
        Endpoint endpoint = server.start();
        try {
            // The authority permits endpoint replacement only after the broker was marked offline.
            authority.registerBroker(brokerId, endpoint);
            node.server = server;
            node.endpoint = endpoint;
            for (Map.Entry<TopicPartition, ReplicaState> entry : node.replicaStates.entrySet()) {
                ClusterAuthority.PartitionState state = authority.partitionState(entry.getKey());
                state.lock.lock();
                try {
                    entry.getValue().update(state.epoch, state.leaderId == brokerId, true);
                } finally {
                    state.lock.unlock();
                }
            }
        } catch (RuntimeException failure) {
            server.close();
            throw failure;
        }
    }

    public synchronized PartitionLog partitionLog(int brokerId, TopicPartition tp) {
        return node(brokerId).catalog.partition(tp);
    }

    public synchronized ReplicaState replicaState(int brokerId, TopicPartition tp) {
        ReplicaState state = node(brokerId).replicaStates.get(tp);
        if (state == null) throw unknownPartition(tp);
        return state;
    }

    public synchronized ReplicationTracker tracker(TopicPartition tp) {
        ReplicationTracker tracker = trackers.get(tp);
        if (tracker == null) throw unknownPartition(tp);
        return tracker;
    }

    public synchronized ReplicatedPartition replicatedPartition(int brokerId, TopicPartition tp) {
        if (!(node(brokerId).catalog.backend(tp) instanceof ReplicatedPartition partition))
            throw new IllegalStateException("partition has no replicated backend");
        return partition;
    }

    /** Creates a follower poller aimed at the leader current at call time. */
    public synchronized FollowerReplicator replicator(int brokerId, TopicPartition tp) {
        BrokerNode follower = node(brokerId);
        int leaderId = authority.metadata(tp).leaderId();
        RpcClient client = new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS);
        return new FollowerReplicator(brokerId, tp, follower.catalog.partition(tp), client,
                replicaState(brokerId, tp));
    }

    /** Runs one TCP replication poll and closes its short-lived client afterward. */
    public int replicateOnce(int brokerId, TopicPartition tp, int maxRecords, int maxBytes) {
        int leaderId = authority.metadata(tp).leaderId();
        try (RpcClient client = new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS)) {
            BrokerNode follower = node(brokerId);
            return new FollowerReplicator(brokerId, tp, follower.catalog.partition(tp), client,
                    replicaState(brokerId, tp)).pollOnce(maxRecords, maxBytes);
        }
    }

    public synchronized PartitionMetadata elect(TopicPartition tp) {
        PartitionMetadata metadata = new LeaderElection(authority).elect(tp);
        for (BrokerNode node : nodes.values()) {
            ReplicaState replica = node.replicaStates.get(tp);
            if (replica != null)
                replica.update(metadata.epoch(), metadata.leaderId() == node.brokerId, authority.isOnline(node.brokerId));
        }
        return metadata;
    }

    public synchronized ReplicaReconciler reconciler(int brokerId, TopicPartition tp) {
        int leaderId = authority.metadata(tp).leaderId();
        return new ReplicaReconciler(brokerId, tp, node(brokerId).catalog.partition(tp),
                new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS), authority);
    }

    public long reconcile(int brokerId, TopicPartition tp) {
        int leaderId = authority.metadata(tp).leaderId();
        try (RpcClient client = new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS)) {
            return new ReplicaReconciler(brokerId, tp, node(brokerId).catalog.partition(tp), client, authority)
                    .reconcile(authority.metadata(tp).epoch());
        }
    }

    public synchronized boolean admit(int brokerId, TopicPartition tp) {
        if (!authority.partitionState(tp).assigned(brokerId))
            throw new CourseException(ErrorCode.INVALID_REQUEST, "broker is not assigned to partition");
        PartitionLog localLog = node(brokerId).catalog.partition(tp);
        return new ReplicaAdmission(authority, localLog).tryAdd(brokerId, tp, authority.metadata(tp).epoch());
    }

    public ReplicationSnapshot snapshot(TopicPartition tp) { return authority.snapshot(tp); }

    public synchronized RpcClient client(int brokerId, int timeoutMillis) {
        if (timeoutMillis <= 0) throw new CourseException(ErrorCode.INVALID_REQUEST, "RPC timeout must be positive");
        return new RpcClient(endpoint(brokerId), timeoutMillis);
    }

    @Override public synchronized void close() {
        if (closed) return;
        closed = true;
        RuntimeException failure = null;
        for (BrokerNode node : nodes.values()) {
            try {
                if (authority.isOnline(node.brokerId)) authority.setBrokerOnline(node.brokerId, false);
                if (node.server != null) node.server.close();
                node.server = null;
            } catch (RuntimeException exception) {
                failure = combine(failure, exception);
            }
        }
        try { if (groups != null) groups.close(); }
        catch (RuntimeException exception) { failure = combine(failure, exception); }
        for (BrokerNode node : nodes.values()) {
            try { node.catalog.close(); }
            catch (RuntimeException exception) { failure = combine(failure, exception); }
        }
        try { if (offsets != null) offsets.close(); }
        catch (RuntimeException exception) { failure = combine(failure, exception); }
        if (failure != null) throw failure;
    }

    private synchronized void closeAfterFailedStart(RuntimeException original) {
        for (BrokerNode node : nodes.values()) {
            try { if (node.server != null) node.server.close(); }
            catch (RuntimeException failure) { original.addSuppressed(failure); }
            try { node.catalog.close(); }
            catch (RuntimeException failure) { original.addSuppressed(failure); }
        }
        try { if (groups != null) groups.close(); }
        catch (RuntimeException failure) { original.addSuppressed(failure); }
        try { if (offsets != null) offsets.close(); }
        catch (RuntimeException failure) { original.addSuppressed(failure); }
        closed = true;
    }

    private BrokerNode node(int brokerId) {
        BrokerNode node = nodes.get(brokerId);
        if (node == null) throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "unknown broker " + brokerId);
        return node;
    }

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("cluster harness is closed");
    }

    private void requireStarted() {
        ensureNotClosed();
        if (!started) throw new IllegalStateException("cluster harness has not been started");
    }

    private static CourseException unknownPartition(TopicPartition tp) {
        return new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown partition " + tp);
    }

    private static RuntimeException combine(RuntimeException old, RuntimeException next) {
        if (old == null) return next;
        old.addSuppressed(next);
        return old;
    }

    private record TopicSpec(int partitions, int minISR) {}

    private static final class BrokerNode {
        private final int brokerId;
        private BrokerServer server;
        private Endpoint endpoint;
        private final PartitionCatalog catalog;
        private final AtomicReference<BrokerHandler> handler;
        private final Map<TopicPartition, ReplicaState> replicaStates = new TreeMap<>();

        private BrokerNode(int brokerId, BrokerServer server, Endpoint endpoint,
                           PartitionCatalog catalog, AtomicReference<BrokerHandler> handler) {
            this.brokerId = brokerId;
            this.server = server;
            this.endpoint = endpoint;
            this.catalog = catalog;
            this.handler = handler;
        }
    }
}
