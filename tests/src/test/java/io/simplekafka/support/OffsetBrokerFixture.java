package io.simplekafka.support;

import io.simplekafka.ErrorCode;
import io.simplekafka.broker.BrokerHandler;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.model.Endpoint;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.BrokerServer;
import io.simplekafka.transport.RpcClient;
import java.nio.file.Path;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicReference;

/** A real TCP broker with a durable offset store but no group-expiration worker. */
public final class OffsetBrokerFixture implements AutoCloseable {
    private final AtomicReference<BrokerHandler> handler = new AtomicReference<>();
    private final BrokerServer server;
    private Endpoint endpoint;
    private PartitionCatalog catalog;
    private OffsetStore offsets;
    private boolean closed;

    public OffsetBrokerFixture(Path root, int brokerId) {
        Objects.requireNonNull(root, "root");
        server = new BrokerServer("127.0.0.1", 0, this::dispatch);
        try {
            endpoint = server.start();
            catalog = new PartitionCatalog(root, brokerId, endpoint);
            offsets = new OffsetStore(root.resolve("test-offset-store"));
            handler.set(new BrokerHandler(catalog, null, null, offsets));
        } catch (RuntimeException failure) {
            close();
            throw failure;
        }
    }

    public Endpoint endpoint() {
        return endpoint;
    }

    public RpcClient client() {
        if (closed) throw new IllegalStateException("offset broker fixture is closed");
        return new RpcClient(endpoint, 3_000);
    }

    public PartitionCatalog catalog() {
        if (catalog == null) throw new IllegalStateException("offset broker fixture is not ready");
        return catalog;
    }

    private Messages.Reply dispatch(Messages.Request request) {
        BrokerHandler current = handler.get();
        if (current == null) return Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "offset broker is not ready");
        return current.handle(request);
    }

    @Override
    public void close() {
        if (closed) return;
        closed = true;
        server.close();
        if (offsets != null) offsets.close();
        if (catalog != null) catalog.close();
        handler.set(null);
    }
}
