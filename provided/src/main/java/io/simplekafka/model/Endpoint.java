package io.simplekafka.model;

import java.util.Objects;

/**
 * A host and TCP port for a broker endpoint.
 *
 * @param host non-blank host string
 * @param port port number from 0 through 65535
 */
public record Endpoint(String host, int port) {
    /**
     * Creates an endpoint with a non-blank host and valid port number.
     *
     * @param host host string
     * @param port port number
     * @throws NullPointerException if {@code host} is null
     * @throws IllegalArgumentException if {@code host} is blank or {@code port} is outside {@code
     *     0..65535}
     */
    public Endpoint {
        Objects.requireNonNull(host, "host");
        if (host.isBlank() || port < 0 || port > 65535)
            throw new IllegalArgumentException("invalid endpoint");
    }

    /**
     * Returns this endpoint as {@code host:port}.
     *
     * @return this endpoint's host and port separated by a colon
     */
    @Override
    public String toString() {
        return host + ":" + port;
    }
}
