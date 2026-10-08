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

    /**
     * Starts a broker with an empty catalog; callers create topics explicitly.
     *
     * @param root directory for broker data
     * @param brokerId broker identifier
     * @param port requested loopback port, or {@code 0} for an OS-assigned port
     * @return an already-started broker harness
     * @throws NullPointerException if {@code root} is {@code null}
     * @throws IllegalArgumentException if {@code brokerId} or {@code port} is out of range
     * @throws io.simplekafka.CourseException if the broker cannot start or open storage
     */
    public static BrokerHarness single(Path root, int brokerId, int port) {
        return new BrokerHarness(root, brokerId, port, null, 0).startAndReturn();
    }

    /**
     * Starts a broker and creates/reopens the requested fixed-partition topic.
     *
     * @param root directory for broker data
     * @param brokerId broker identifier
     * @param port requested loopback port, or {@code 0} for an OS-assigned port
     * @param topic topic identifier to create or reopen
     * @param partitions number of partitions in the topic
     * @return an already-started broker harness with the topic available
     * @throws NullPointerException if {@code root} or {@code topic} is {@code null}
     * @throws IllegalArgumentException if broker, port, partition count, or topic identifier is
     *     invalid
     * @throws io.simplekafka.CourseException if the broker cannot start or open storage
     */
    public static BrokerHarness single(
            Path root, int brokerId, int port, String topic, int partitions) {
        return new BrokerHarness(
                        root, brokerId, port, Objects.requireNonNull(topic, "topic"), partitions)
                .startAndReturn();
    }

    /**
     * Starts a broker with durable offsets and group coordination enabled.
     *
     * <p>Group expiry runs on a daemon worker using the supplied clock.
     *
     * @param root directory for broker data and durable offsets
     * @param brokerId broker identifier
     * @param port requested loopback port, or {@code 0} for an OS-assigned port
     * @param clock clock used by group coordination
     * @param sessionTimeoutMillis group session timeout in milliseconds
     * @return an already-started broker harness
     * @throws NullPointerException if {@code root} or {@code clock} is {@code null}
     * @throws IllegalArgumentException if the broker, port, or session timeout is out of range
     * @throws io.simplekafka.CourseException if the broker cannot start or open storage
     */
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

    /**
     * Starts the full replicated teaching harness using the shared authority.
     *
     * @param root directory for broker logs and group offsets
     * @param clock clock used by group expiry and replication timing
     * @return an already-started replicated harness
     * @throws NullPointerException if {@code root} or {@code clock} is {@code null}
     * @throws io.simplekafka.CourseException if the harness cannot create storage or start
     */
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

    /**
     * Starts the replicated teaching harness using the system wall clock.
     *
     * @param root directory for broker logs and group offsets
     * @return an already-started replicated harness
     * @throws NullPointerException if {@code root} is {@code null}
     * @throws io.simplekafka.CourseException if the harness cannot create storage or start
     */
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

    /**
     * Starts once and returns the actual loopback endpoint selected by the OS.
     *
     * <p>Repeated calls before close return the same endpoint. On initialization failure this
     * harness closes partial resources and rethrows the failure.
     *
     * @return the broker endpoint
     * @throws IllegalStateException if the harness has been closed
     * @throws io.simplekafka.CourseException if the server cannot bind or broker storage cannot
     *     initialize
     */
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

    /**
     * Returns the endpoint selected when this harness started.
     *
     * @return the loopback broker endpoint
     * @throws IllegalStateException if the broker has not started successfully
     */
    public synchronized Endpoint endpoint() {
        if (endpoint == null) throw new IllegalStateException("broker harness has not started");
        return endpoint;
    }

    /**
     * Returns the local partition catalog created at start.
     *
     * @return this broker's catalog
     * @throws IllegalStateException if the broker has not started or has been closed
     */
    public synchronized PartitionCatalog catalog() {
        if (catalog == null) throw new IllegalStateException("broker harness has not started");
        return catalog;
    }

    /**
     * Returns the broker identifier configured for this harness.
     *
     * @return broker identifier
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * Runs deterministic group expiry for tests using an injected clock.
     *
     * @return expired group/member identifiers, such as {@code group/member}
     * @throws IllegalStateException if group coordination is disabled
     */
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

    /**
     * Closes the server, expiry worker, group resources, offsets, and catalog.
     *
     * <p>Closing is idempotent; later close failures are suppressed on the first failure.
     *
     * @throws RuntimeException if closing a group, offset, or catalog resource fails
     */
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
