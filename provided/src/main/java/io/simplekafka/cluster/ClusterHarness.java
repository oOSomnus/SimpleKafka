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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
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
    private final Map<TopicPartition, AckPolicy> ackPolicies = new TreeMap<>();
    private final Map<String, TopicSpec> topics = new TreeMap<>();
    private OffsetStore offsets;
    private GroupCoordinator groups;
    private ScheduledExecutorService groupExpiry;
    private boolean started;
    private boolean closed;

    /**
     * Creates a harness rooted at the given directory using the system wall clock.
     *
     * @param root directory for broker logs and group offsets
     * @throws NullPointerException if {@code root} is {@code null}
     * @throws CourseException with {@code STORAGE_ERROR} if the root or offset store cannot be
     *     created
     */
    public ClusterHarness(Path root) {
        this(root, SystemTimeSource.INSTANCE);
    }

    /**
     * Creates a harness rooted at the given directory with an injected clock.
     *
     * @param root directory for broker logs and group offsets
     * @param clock clock used by group expiry and replication timing
     * @throws NullPointerException if {@code root} or {@code clock} is {@code null}
     * @throws CourseException with {@code STORAGE_ERROR} if the root or offset store cannot be
     *     created
     */
    public ClusterHarness(Path root, TimeSource clock) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        this.clock = Objects.requireNonNull(clock, "clock");
        this.authority = new ClusterAuthority(clock);
        try {
            Files.createDirectories(this.root);
            this.offsets = new OffsetStore(this.root.resolve("group-offsets"));
        } catch (IOException exception) {
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR, "cannot create cluster harness root", exception);
        }
    }

    /**
     * Starts three independent loopback servers, each bound to an OS-assigned port.
     *
     * <p>Starting an already-started harness is a no-op. A startup failure closes partial state
     * before the failure is rethrown.
     *
     * @throws IllegalStateException if the harness has been closed
     * @throws CourseException with {@code STORAGE_ERROR} if a broker cannot bind or initialize
     */
    public synchronized void start() {
        ensureNotClosed();
        if (started) return;
        try {
            for (int brokerId : BROKER_IDS) {
                AtomicReference<BrokerHandler> handler = new AtomicReference<>();
                BrokerServer server =
                        new BrokerServer(
                                "127.0.0.1",
                                0,
                                request -> {
                                    BrokerHandler current = handler.get();
                                    return current == null
                                            ? io.simplekafka.protocol.Messages.Reply.failure(
                                                    ErrorCode.STORAGE_ERROR,
                                                    "broker initialization is incomplete")
                                            : current.handle(request);
                                });
                Endpoint endpoint = server.start();
                try {
                    authority.registerBroker(brokerId, endpoint);
                    Path brokerRoot = root.resolve("broker-" + brokerId);
                    PartitionCatalog catalog = new PartitionCatalog(brokerRoot, brokerId, endpoint);
                    nodes.put(
                            brokerId, new BrokerNode(brokerId, server, endpoint, catalog, handler));
                } catch (RuntimeException failure) {
                    server.close();
                    throw failure;
                }
            }
            PartitionCatalog coordinatorCatalog = nodes.get(2).catalog;
            groups =
                    new GroupCoordinator(
                            coordinatorCatalog,
                            new RoundRobinAssignor(),
                            clock,
                            DEFAULT_GROUP_TIMEOUT_MILLIS);
            GroupCoordinator coordinator = groups;
            groupExpiry =
                    Executors.newSingleThreadScheduledExecutor(
                            task -> {
                                Thread worker =
                                        new Thread(task, "simple-kafka-cluster-group-expiry");
                                worker.setDaemon(true);
                                return worker;
                            });
            long expiryPeriodMillis = Math.max(1, Math.min(DEFAULT_GROUP_TIMEOUT_MILLIS / 2, 250));
            groupExpiry.scheduleAtFixedRate(
                    coordinator::expire,
                    expiryPeriodMillis,
                    expiryPeriodMillis,
                    TimeUnit.MILLISECONDS);
            for (BrokerNode node : nodes.values())
                node.handler.set(new BrokerHandler(node.catalog, authority, groups, offsets));
            started = true;
        } catch (RuntimeException failure) {
            closeAfterFailedStart(failure);
            throw failure;
        }
    }

    /**
     * Runs deterministic group expiry for tests using an injected clock.
     *
     * @return expired group/member identifiers, such as {@code group/member}
     * @throws IllegalStateException if the harness has not been started or has been closed
     */
    public synchronized Set<String> expireGroups() {
        requireStarted();
        return groups.expire();
    }

    /**
     * Creates fixed RF3 partitions: leaders rotate 1,2,3 and every replica starts in the ISR.
     *
     * <p>Repeating the same configuration is a no-op; changing it is rejected.
     *
     * @param topic topic identifier to create
     * @param partitions number of partitions to create
     * @param minISR minimum in-sync replica count for writes
     * @throws IllegalStateException if the harness has not been started or has been closed
     * @throws IllegalArgumentException if {@code topic} is not a valid topic identifier
     * @throws CourseException with {@code INVALID_REQUEST} for invalid or changed configuration, or
     *     with {@code NOT_LEADER} if any broker is offline
     */
    public synchronized void createTopic(String topic, int partitions, int minISR) {
        requireStarted();
        if (topic == null || partitions <= 0 || minISR < 1 || minISR > BROKER_IDS.size())
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid replicated topic configuration");
        TopicSpec existing = topics.get(topic);
        TopicSpec requested = new TopicSpec(partitions, minISR);
        if (existing != null) {
            if (!existing.equals(requested))
                throw new CourseException(ErrorCode.INVALID_REQUEST, "topic configuration changed");
            return;
        }
        for (BrokerNode node : nodes.values()) {
            if (!authority.isOnline(node.brokerId))
                throw new CourseException(
                        ErrorCode.NOT_LEADER,
                        "all brokers must be online when creating a replicated topic");
            node.catalog.createTopic(topic, partitions);
        }
        List<Integer> replicaIds = List.copyOf(BROKER_IDS);
        for (int partition = 0; partition < partitions; partition++) {
            TopicPartition tp = new TopicPartition(topic, partition);
            int leaderId = BROKER_IDS.get(partition % BROKER_IDS.size());
            authority.createPartition(tp, leaderId, replicaIds, minISR);
            ReplicationTracker tracker =
                    new ReplicationTracker(
                            authority,
                            tp,
                            leaderId,
                            Set.copyOf(replicaIds),
                            minISR,
                            clock,
                            DEFAULT_LAG_TIMEOUT_MILLIS);
            AckPolicy policy = new AckPolicy(tracker);
            ackPolicies.put(tp, policy);
            trackers.put(tp, tracker);
            for (BrokerNode node : nodes.values()) {
                ReplicaState replica =
                        new ReplicaState(node.brokerId, 0, node.brokerId == leaderId, true);
                ReplicatedPartition backend =
                        new ReplicatedPartition(
                                tp, node.catalog.partition(tp), replica, tracker, policy);
                node.replicaStates.put(tp, replica);
                node.catalog.installBackend(tp, backend);
            }
        }
        topics.put(topic, requested);
    }

    /**
     * Returns a broker's current endpoint.
     *
     * @param brokerId broker whose endpoint to return
     * @return current endpoint, which may change after a broker restart
     * @throws CourseException with {@code UNKNOWN_MEMBER} if the broker is unknown
     */
    public synchronized Endpoint endpoint(int brokerId) {
        return node(brokerId).endpoint;
    }

    /**
     * Returns the broker's catalog and its local partition logs.
     *
     * @param brokerId broker whose catalog to return
     * @return the broker's partition catalog
     * @throws CourseException with {@code UNKNOWN_MEMBER} if the broker is unknown
     */
    public synchronized PartitionCatalog catalog(int brokerId) {
        return node(brokerId).catalog;
    }

    /**
     * Returns the shared single-JVM cluster authority.
     *
     * @return the harness authority
     */
    public ClusterAuthority authority() {
        return authority;
    }

    /**
     * Stops only this broker's TCP server; authority, catalog, log handles, and peers stay alive.
     *
     * <p>It marks the broker and its replicas offline but retains the catalog and log handles.
     *
     * <p>Stopping a broker that is already stopped is a no-op.
     *
     * @param brokerId broker to stop
     * @throws IllegalStateException if the harness has not been started or has been closed
     * @throws CourseException with {@code UNKNOWN_MEMBER} if the broker is unknown
     */
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

    /**
     * Restarts the broker socket on a fresh ephemeral port and preserves its independent
     * disk/catalog.
     *
     * <p>Calling this for a running broker is a no-op; the endpoint changes after a restart.
     *
     * @param brokerId broker to restart
     * @throws IllegalStateException if the harness has not been started or has been closed
     * @throws CourseException with {@code UNKNOWN_MEMBER} if the broker is unknown, or with {@code
     *     STORAGE_ERROR} if the new server cannot bind
     */
    public synchronized void restartBroker(int brokerId) {
        requireStarted();
        BrokerNode node = node(brokerId);
        if (node.server != null) return;
        AtomicReference<BrokerHandler> handler = node.handler;
        BrokerServer server =
                new BrokerServer(
                        "127.0.0.1",
                        0,
                        request -> {
                            BrokerHandler current = handler.get();
                            return current == null
                                    ? io.simplekafka.protocol.Messages.Reply.failure(
                                            ErrorCode.STORAGE_ERROR,
                                            "broker initialization is incomplete")
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

    /**
     * Replaces one broker's log and replica backends with fresh objects recovered from the same
     * catalog paths, then restarts its socket. Authority, group coordinator, and offset store
     * remain in memory; reopened followers must re-replicate and prove recovery before ISR
     * admission.
     */
    public synchronized void reopenBrokerFromDisk(int brokerId) {
        requireStarted();
        BrokerNode node = node(brokerId);
        stopBroker(brokerId);
        node.handler.set(null);
        authority.setBrokerOnline(brokerId, false);
        for (ReplicaState replica : node.replicaStates.values())
            replica.update(replica.epoch(), replica.isLeader(), false);
        List<PartitionLog> reopenedLogs = new ArrayList<>();
        try {
            for (Map.Entry<String, TopicSpec> topic : topics.entrySet()) {
                for (int partition = 0; partition < topic.getValue().partitions(); partition++) {
                    TopicPartition tp = new TopicPartition(topic.getKey(), partition);
                    PartitionLog log = node.catalog.reopenPartition(tp);
                    reopenedLogs.add(log);
                    ReplicationTracker tracker =
                            Objects.requireNonNull(trackers.get(tp), "tracker");
                    AckPolicy policy = Objects.requireNonNull(ackPolicies.get(tp), "ack policy");
                    ClusterAuthority.PartitionState state = authority.partitionState(tp);
                    state.lock.lock();
                    try {
                        boolean leader = state.leaderId == brokerId;
                        if (leader) {
                            long leo = log.logEndOffset();
                            long reported = state.reportedLEO.getOrDefault(brokerId, 0L);
                            if (leo < state.highWatermark || leo < reported)
                                throw new CourseException(
                                        ErrorCode.CORRUPT_RECORD,
                                        "reopened leader log is behind committed or reported progress");
                        } else {
                            state.isr.remove(brokerId);
                            state.reportedLEO.put(brokerId, 0L);
                            state.lastCaughtUpMillis.remove(brokerId);
                            state.changed.signalAll();
                        }
                        RecoveryProofRegistry.clear(authority, tp, brokerId);
                        ReplicaState replica =
                                new ReplicaState(brokerId, state.epoch, leader, false);
                        node.replicaStates.put(tp, replica);
                        node.catalog.installBackend(
                                tp, new ReplicatedPartition(tp, log, replica, tracker, policy));
                    } finally {
                        state.lock.unlock();
                    }
                }
            }
            node.handler.set(new BrokerHandler(node.catalog, authority, groups, offsets));
            restartBroker(brokerId);
        } catch (RuntimeException failure) {
            node.handler.set(null);
            try {
                authority.setBrokerOnline(brokerId, false);
            } catch (RuntimeException offlineFailure) {
                failure.addSuppressed(offlineFailure);
            }
            if (node.server != null) {
                try {
                    node.server.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
                node.server = null;
            }
            for (PartitionLog log : reopenedLogs) {
                try {
                    log.close();
                } catch (RuntimeException closeFailure) {
                    failure.addSuppressed(closeFailure);
                }
            }
            throw failure;
        }
    }

    /**
     * Returns the local partition log for a broker.
     *
     * @param brokerId broker whose log to return
     * @param tp partition whose log to return
     * @return the broker's local partition log
     * @throws CourseException if the broker or partition is unknown
     */
    public synchronized PartitionLog partitionLog(int brokerId, TopicPartition tp) {
        return node(brokerId).catalog.partition(tp);
    }

    /**
     * Returns a broker's role and liveness mirror for a partition.
     *
     * @param brokerId broker whose replica state to return
     * @param tp partition whose replica state to return
     * @return the broker's replica state
     * @throws CourseException if the broker is unknown or the partition is not tracked
     */
    public synchronized ReplicaState replicaState(int brokerId, TopicPartition tp) {
        ReplicaState state = node(brokerId).replicaStates.get(tp);
        if (state == null) throw unknownPartition(tp);
        return state;
    }

    /**
     * Returns the replication tracker for a partition.
     *
     * @param tp partition whose tracker to return
     * @return the partition's replication tracker
     * @throws CourseException with {@code UNKNOWN_TOPIC_OR_PARTITION} if the partition is unknown
     */
    public synchronized ReplicationTracker tracker(TopicPartition tp) {
        ReplicationTracker tracker = trackers.get(tp);
        if (tracker == null) throw unknownPartition(tp);
        return tracker;
    }

    /**
     * Returns the replicated backend installed for a broker's partition.
     *
     * @param brokerId broker whose backend to inspect
     * @param tp partition whose backend to return
     * @return the replicated partition backend
     * @throws CourseException if the broker or partition is unknown
     * @throws IllegalStateException if the partition has no replicated backend
     */
    public synchronized ReplicatedPartition replicatedPartition(int brokerId, TopicPartition tp) {
        if (!(node(brokerId).catalog.backend(tp) instanceof ReplicatedPartition partition))
            throw new IllegalStateException("partition has no replicated backend");
        return partition;
    }

    /**
     * Creates a follower poller aimed at the leader current at call time.
     *
     * @param brokerId follower broker to poll
     * @param tp partition to replicate
     * @return a poller retaining a client for the selected leader
     * @throws CourseException with {@code UNKNOWN_MEMBER} or {@code UNKNOWN_TOPIC_OR_PARTITION} if
     *     the broker or partition is unknown
     */
    public synchronized FollowerReplicator replicator(int brokerId, TopicPartition tp) {
        BrokerNode follower = node(brokerId);
        int leaderId = authority.metadata(tp).leaderId();
        RpcClient client = new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS);
        return new FollowerReplicator(
                brokerId, tp, follower.catalog.partition(tp), client, replicaState(brokerId, tp));
    }

    /**
     * Runs one TCP replication poll and closes its short-lived client afterward.
     *
     * <p>The client is closed after the poll, and this method does not report the resulting log end
     * to the replication tracker. The exercise poll currently throws {@link
     * io.simplekafka.ExerciseNotImplementedException}.
     *
     * @param brokerId follower broker to poll
     * @param tp partition to replicate
     * @param maxRecords maximum records requested by the follower poll
     * @param maxBytes maximum bytes requested by the follower poll
     * @return number of records appended by the poll
     * @throws CourseException if the broker or partition is unknown or the RPC fails
     * @throws io.simplekafka.ExerciseNotImplementedException while follower replication is
     *     unimplemented
     */
    public int replicateOnce(int brokerId, TopicPartition tp, int maxRecords, int maxBytes) {
        int leaderId = authority.metadata(tp).leaderId();
        try (RpcClient client = new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS)) {
            BrokerNode follower = node(brokerId);
            return new FollowerReplicator(
                            brokerId,
                            tp,
                            follower.catalog.partition(tp),
                            client,
                            replicaState(brokerId, tp))
                    .pollOnce(maxRecords, maxBytes);
        }
    }

    /**
     * Elects a clean replica when needed and refreshes the brokers' role mirrors.
     *
     * <p>An online leader remains selected. Otherwise the intended election selects the lowest
     * online broker in the old ISR whose log end is at least the high watermark, advances the
     * epoch, and makes only that broker the ISR. The exercise method currently throws {@link
     * io.simplekafka.ExerciseNotImplementedException}.
     *
     * @param tp partition whose leader may be elected
     * @return metadata for the current or newly elected leader
     * @throws CourseException with {@code NO_ELIGIBLE_LEADER} if no clean candidate exists
     * @throws io.simplekafka.ExerciseNotImplementedException while leader election is unimplemented
     */
    public synchronized PartitionMetadata elect(TopicPartition tp) {
        PartitionMetadata metadata = new LeaderElection(authority).elect(tp);
        for (BrokerNode node : nodes.values()) {
            ReplicaState replica = node.replicaStates.get(tp);
            if (replica != null)
                replica.update(
                        metadata.epoch(),
                        metadata.leaderId() == node.brokerId,
                        authority.isOnline(node.brokerId));
        }
        return metadata;
    }

    /**
     * Creates a reconciler bound to the leader endpoint selected at call time.
     *
     * @param brokerId replica broker to reconcile
     * @param tp partition to reconcile
     * @return reconciler holding a client for the selected leader
     * @throws CourseException with {@code UNKNOWN_MEMBER} or {@code UNKNOWN_TOPIC_OR_PARTITION} if
     *     the broker or partition is unknown
     */
    public synchronized ReplicaReconciler reconciler(int brokerId, TopicPartition tp) {
        int leaderId = authority.metadata(tp).leaderId();
        return new ReplicaReconciler(
                brokerId,
                tp,
                node(brokerId).catalog.partition(tp),
                new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS),
                authority);
    }

    /**
     * Reconciles a replica with the leader selected at call time and returns its repaired log end.
     *
     * <p>The temporary RPC client is closed after the attempt. The exercise method currently throws
     * {@link io.simplekafka.ExerciseNotImplementedException}.
     *
     * @param brokerId replica broker to reconcile
     * @param tp partition to reconcile
     * @return local log-end offset after reconciliation
     * @throws CourseException if the broker or partition is unknown or the RPC fails
     * @throws io.simplekafka.ExerciseNotImplementedException while reconciliation is unimplemented
     */
    public long reconcile(int brokerId, TopicPartition tp) {
        int leaderId = authority.metadata(tp).leaderId();
        try (RpcClient client = new RpcClient(endpoint(leaderId), DEFAULT_RPC_TIMEOUT_MILLIS)) {
            return new ReplicaReconciler(
                            brokerId, tp, node(brokerId).catalog.partition(tp), client, authority)
                    .reconcile(authority.metadata(tp).epoch());
        }
    }

    /**
     * Attempts to admit an assigned replica to the ISR using its current recovery proof.
     *
     * <p>Admission requires proof for the current epoch that matches the local log identity,
     * mutation version, and leader log end. The exercise method currently throws {@link
     * io.simplekafka.ExerciseNotImplementedException}.
     *
     * @param brokerId replica broker to admit
     * @param tp partition for which to attempt admission
     * @return {@code true} if admitted or already in the ISR; {@code false} if the proof is not
     *     current and complete
     * @throws CourseException with {@code INVALID_REQUEST} if the broker is not assigned, or with
     *     {@code UNKNOWN_TOPIC_OR_PARTITION} if the partition is absent
     * @throws io.simplekafka.ExerciseNotImplementedException while the admission exercise is
     *     unimplemented
     */
    public synchronized boolean admit(int brokerId, TopicPartition tp) {
        if (!authority.partitionState(tp).assigned(brokerId))
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "broker is not assigned to partition");
        PartitionLog localLog = node(brokerId).catalog.partition(tp);
        return new ReplicaAdmission(authority, localLog)
                .tryAdd(brokerId, tp, authority.metadata(tp).epoch());
    }

    /**
     * Returns a consistent replication snapshot for a partition.
     *
     * @param tp partition to snapshot
     * @return copied replication state
     * @throws CourseException with {@code UNKNOWN_TOPIC_OR_PARTITION} if the partition is absent
     */
    public ReplicationSnapshot snapshot(TopicPartition tp) {
        return authority.snapshot(tp);
    }

    /**
     * Creates a client bound to the broker's current endpoint.
     *
     * @param brokerId broker to contact
     * @param timeoutMillis connect and read timeout in milliseconds
     * @return a client owned by the caller
     * @throws CourseException with {@code INVALID_REQUEST} if the timeout is not positive, or with
     *     {@code UNKNOWN_MEMBER} if the broker is unknown
     */
    public synchronized RpcClient client(int brokerId, int timeoutMillis) {
        if (timeoutMillis <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "RPC timeout must be positive");
        return new RpcClient(endpoint(brokerId), timeoutMillis);
    }

    /**
     * Closes the harness and its brokers, group coordinator, catalogs, and offset store.
     *
     * <p>Closing is idempotent. If multiple resources fail to close, the first runtime failure is
     * thrown and later failures are suppressed on it.
     *
     * @throws RuntimeException if closing a resource fails
     */
    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        stopGroupExpiry();
        RuntimeException failure = null;
        for (BrokerNode node : nodes.values()) {
            try {
                if (authority.isOnline(node.brokerId))
                    authority.setBrokerOnline(node.brokerId, false);
                if (node.server != null) node.server.close();
                node.server = null;
            } catch (RuntimeException exception) {
                failure = combine(failure, exception);
            }
        }
        try {
            if (groups != null) groups.close();
        } catch (RuntimeException exception) {
            failure = combine(failure, exception);
        }
        for (BrokerNode node : nodes.values()) {
            try {
                node.catalog.close();
            } catch (RuntimeException exception) {
                failure = combine(failure, exception);
            }
        }
        try {
            if (offsets != null) offsets.close();
        } catch (RuntimeException exception) {
            failure = combine(failure, exception);
        }
        if (failure != null) throw failure;
    }

    private synchronized void closeAfterFailedStart(RuntimeException original) {
        stopGroupExpiry();
        for (BrokerNode node : nodes.values()) {
            try {
                if (node.server != null) node.server.close();
            } catch (RuntimeException failure) {
                original.addSuppressed(failure);
            }
            try {
                node.catalog.close();
            } catch (RuntimeException failure) {
                original.addSuppressed(failure);
            }
        }
        try {
            if (groups != null) groups.close();
        } catch (RuntimeException failure) {
            original.addSuppressed(failure);
        }
        try {
            if (offsets != null) offsets.close();
        } catch (RuntimeException failure) {
            original.addSuppressed(failure);
        }
        closed = true;
    }

    private BrokerNode node(int brokerId) {
        BrokerNode node = nodes.get(brokerId);
        if (node == null)
            throw new CourseException(ErrorCode.UNKNOWN_MEMBER, "unknown broker " + brokerId);
        return node;
    }

    private void ensureNotClosed() {
        if (closed) throw new IllegalStateException("cluster harness is closed");
    }

    private void requireStarted() {
        ensureNotClosed();
        if (!started) throw new IllegalStateException("cluster harness has not been started");
    }

    private void stopGroupExpiry() {
        if (groupExpiry == null) return;
        groupExpiry.shutdownNow();
        try {
            groupExpiry.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
        groupExpiry = null;
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

        private BrokerNode(
                int brokerId,
                BrokerServer server,
                Endpoint endpoint,
                PartitionCatalog catalog,
                AtomicReference<BrokerHandler> handler) {
            this.brokerId = brokerId;
            this.server = server;
            this.endpoint = endpoint;
            this.catalog = catalog;
            this.handler = handler;
        }
    }
}
