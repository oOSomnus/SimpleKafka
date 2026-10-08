package io.simplekafka.support;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.cluster.ClusterHarness;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.Endpoint;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.BrokerServer;

import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/** Owns a loopback-only single broker and all of its local partition logs. */
public final class BrokerHarness implements AutoCloseable {
    private final Path root;
    private final int brokerId;
    private final int port;
    private final String initialTopic;
    private final int initialPartitions;
    private final TimeSource groupClock;
    private final long groupSessionTimeoutMillis;
    private GroupCoordinator groups;
    private OffsetStore offsets;
    private ScheduledExecutorService groupExpiry;
    private final BrokerServer server;
    private PartitionCatalog catalog;
    private BrokerHandler handler;
    private Endpoint endpoint;
    private boolean closed;

    private BrokerHarness(
            Path root, int brokerId, int port, String initialTopic, int initialPartitions) {
        this(root, brokerId, port, initialTopic, initialPartitions, null, 0);
    }

    private BrokerHarness(
            Path root,
            int brokerId,
            int port,
            String initialTopic,
            int initialPartitions,
            TimeSource groupClock,
            long groupSessionTimeoutMillis) {
        this.root = Objects.requireNonNull(root, "root").toAbsolutePath().normalize();
        if (brokerId < 0) throw new IllegalArgumentException("brokerId must be nonnegative");
        if (port < 0 || port > 65535) throw new IllegalArgumentException("invalid port");
        if (initialTopic != null && initialPartitions <= 0)
            throw new IllegalArgumentException("partitions must be positive");
        if (groupClock != null && groupSessionTimeoutMillis <= 0)
            throw new IllegalArgumentException("session timeout must be positive");
        this.brokerId = brokerId;
        this.port = port;
        this.initialTopic = initialTopic;
        this.initialPartitions = initialPartitions;
        this.groupClock = groupClock;
        this.groupSessionTimeoutMillis = groupSessionTimeoutMillis;
        this.server = new BrokerServer("127.0.0.1", port, this::dispatch);
    }

    /** Starts a broker with an empty catalog; callers create topics explicitly. */
    public static BrokerHarness single(Path root, int brokerId, int port) {
        return new BrokerHarness(root, brokerId, port, null, 0).startAndReturn();
    }

    /** Starts a broker and creates/reopens the requested fixed-partition topic. */
    public static BrokerHarness single(
            Path root, int brokerId, int port, String topic, int partitions) {
        return new BrokerHarness(
                        root, brokerId, port, Objects.requireNonNull(topic, "topic"), partitions)
                .startAndReturn();
    }

    /** Starts a single broker with durable offsets and group coordination enabled. */
    public static BrokerHarness single(
            Path root, int brokerId, int port, TimeSource clock, long sessionTimeoutMillis) {
        return new BrokerHarness(
                        root,
                        brokerId,
                        port,
                        null,
                        0,
                        Objects.requireNonNull(clock, "clock"),
                        sessionTimeoutMillis)
                .startAndReturn();
    }

    /** Starts the full replicated teaching harness using the shared authority. */
    public static ClusterHarness replicated(Path root, TimeSource clock) {
        ClusterHarness harness = new ClusterHarness(root, Objects.requireNonNull(clock, "clock"));
        try {
            harness.start();
            return harness;
        } catch (RuntimeException failure) {
            harness.close();
            throw failure;
        }
    }

    public static ClusterHarness replicated(Path root) {
        return replicated(root, SystemTimeSource.INSTANCE);
    }

    private BrokerHarness startAndReturn() {
        try {
            start();
            return this;
        } catch (RuntimeException failure) {
            close();
            throw failure;
        }
    }

    /** Starts once and returns the actual loopback endpoint selected by the OS. */
    public synchronized Endpoint start() {
        if (closed) throw new IllegalStateException("broker harness is closed");
        if (endpoint != null) return endpoint;
        endpoint = server.start();
        try {
            catalog = new PartitionCatalog(root, brokerId, endpoint);
            if (initialTopic != null) catalog.createTopic(initialTopic, initialPartitions);
            if (groupClock != null) {
                offsets = new OffsetStore(root.resolve("offsets.log"));
                groups =
                        new GroupCoordinator(
                                catalog,
                                new RoundRobinAssignor(),
                                groupClock,
                                groupSessionTimeoutMillis);
                groupExpiry =
                        Executors.newSingleThreadScheduledExecutor(
                                task -> {
                                    Thread thread =
                                            new Thread(
                                                    task, "simple-kafka-group-expiry-" + brokerId);
                                    thread.setDaemon(true);
                                    return thread;
                                });
                long periodMillis = Math.max(1, Math.min(groupSessionTimeoutMillis / 2, 250));
                groupExpiry.scheduleAtFixedRate(
                        () -> {
                            GroupCoordinator current = groups;
                            if (current != null) current.expire();
                        },
                        periodMillis,
                        periodMillis,
                        TimeUnit.MILLISECONDS);
            }
            handler = new BrokerHandler(catalog, null, groups, offsets);
            return endpoint;
        } catch (RuntimeException failure) {
            close();
            endpoint = null;
            throw failure;
        }
    }

    public synchronized Endpoint endpoint() {
        if (endpoint == null) throw new IllegalStateException("broker harness has not started");
        return endpoint;
    }

    public synchronized PartitionCatalog catalog() {
        if (catalog == null) throw new IllegalStateException("broker harness has not started");
        return catalog;
    }

    public int brokerId() {
        return brokerId;
    }

    /** Runs deterministic group expiry for tests using an injected clock. */
    public java.util.Set<String> expireGroups() {
        GroupCoordinator current;
        synchronized (this) {
            current = groups;
        }
        if (current == null) throw new IllegalStateException("group coordination is disabled");
        return current.expire();
    }

    private Messages.Reply dispatch(Messages.Request request) {
        BrokerHandler current;
        synchronized (this) {
            current = handler;
        }
        if (current == null)
            return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "broker is not ready");
        return current.handle(request);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        server.close();
        if (groupExpiry != null) {
            groupExpiry.shutdownNow();
            try {
                groupExpiry.awaitTermination(2, TimeUnit.SECONDS);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
        handler = null;
        RuntimeException failure = null;
        if (groups != null) {
            try {
                groups.close();
            } catch (RuntimeException exception) {
                failure = exception;
            }
            groups = null;
        }
        if (offsets != null) {
            try {
                offsets.close();
            } catch (RuntimeException exception) {
                if (failure == null) failure = exception;
                else failure.addSuppressed(exception);
            }
            offsets = null;
        }
        if (catalog != null) {
            try {
                catalog.close();
            } catch (RuntimeException exception) {
                if (failure == null) failure = exception;
                else failure.addSuppressed(exception);
            }
            catalog = null;
        }
        if (failure != null) throw failure;
    }
}
