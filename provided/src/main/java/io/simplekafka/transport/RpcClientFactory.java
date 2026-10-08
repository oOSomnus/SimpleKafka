package io.simplekafka.transport;

import io.simplekafka.model.Endpoint;

/** Supplies RPC clients for endpoint-specific connections. */
@FunctionalInterface
public interface RpcClientFactory {
    /**
     * Creates or obtains a client for the requested endpoint.
     *
     * @param endpoint destination for the client
     * @return client for that endpoint; ownership transfers to the caller
     */
    RpcClient connect(Endpoint endpoint);
}
