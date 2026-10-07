package io.simplekafka.transport;

import io.simplekafka.model.Endpoint;

@FunctionalInterface
public interface RpcClientFactory {
    RpcClient connect(Endpoint endpoint);
}
