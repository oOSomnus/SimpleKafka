package io.simplekafka.model;

import java.util.Objects;

public record Endpoint(String host, int port) {
    public Endpoint {
        Objects.requireNonNull(host, "host");
        if (host.isBlank() || port < 0 || port > 65535)
            throw new IllegalArgumentException("invalid endpoint");
    }

    @Override
    public String toString() {
        return host + ":" + port;
    }
}
