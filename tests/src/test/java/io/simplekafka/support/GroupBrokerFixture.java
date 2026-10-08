package io.simplekafka.support;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.group.RoundRobinAssignor;
import io.simplekafka.model.Endpoint;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;

import java.nio.file.Path;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Real TCP group broker without a background expiry worker, so each lesson controls time
 * explicitly.
 */
public final class GroupBrokerFixture implements AutoCloseable {
    private final AtomicReference<BrokerHandler> handler = new AtomicReference<>();
    private final BrokerServer server;
    private Endpoint endpoint;
    private PartitionCatalog catalog;
    private OffsetStore offsets;
    private GroupCoordinator groups;
    private boolean closed;

    public GroupBrokerFixture(
            Path root,
            int brokerId,
            String topic,
            int partitions,
            TimeSource clock,
            long sessionTimeoutMillis) {
        Objects.requireNonNull(root, "root");
        server = new BrokerServer("127.0.0.1", 0, this::dispatch);
        try {
            endpoint = server.start();
            catalog = new PartitionCatalog(root, brokerId, endpoint);
            catalog.createTopic(topic, partitions);
            offsets = new OffsetStore(root.resolve("group-offsets"));
            groups =
                    new GroupCoordinator(
                            catalog, new RoundRobinAssignor(), clock, sessionTimeoutMillis);
            handler.set(new BrokerHandler(catalog, null, groups, offsets));
        } catch (RuntimeException failure) {
            closeAfterFailure(failure);
            throw failure;
        }
    }

    public RpcClient client() {
        if (closed) throw new IllegalStateException("group broker fixture is closed");
        return new RpcClient(endpoint, 3_000);
    }

    /** Deterministically runs the same expiry transition without a scheduled background task. */
    public Set<String> expireGroups() {
        if (groups == null || closed)
            throw new IllegalStateException("group broker fixture is not active");
        return groups.expire();
    }

    private Messages.Reply dispatch(Messages.Request request) {
        BrokerHandler current = handler.get();
        if (current == null)
            return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "group broker is not ready");
        return current.handle(request);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        RuntimeException failure = null;
        try {
            server.close();
        } catch (RuntimeException exception) {
            failure = exception;
        }
        if (groups != null) {
            try {
                groups.close();
            } catch (RuntimeException exception) {
                failure = combine(failure, exception);
            }
            groups = null;
        }
        if (offsets != null) {
            try {
                offsets.close();
            } catch (RuntimeException exception) {
                failure = combine(failure, exception);
            }
            offsets = null;
        }
        if (catalog != null) {
            try {
                catalog.close();
            } catch (RuntimeException exception) {
                failure = combine(failure, exception);
            }
            catalog = null;
        }
        handler.set(null);
        if (failure != null) throw failure;
    }

    private void closeAfterFailure(RuntimeException original) {
        try {
            close();
        } catch (RuntimeException failure) {
            original.addSuppressed(failure);
        }
    }

    private static RuntimeException combine(RuntimeException old, RuntimeException next) {
        if (old == null) return next;
        old.addSuppressed(next);
        return old;
    }
}
