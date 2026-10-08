package io.simplekafka.protocol;

import io.simplekafka.ErrorCode;

import java.util.Objects;

/** Frame payload ownership transfers to the frame at construction. */
public record Frame(short api, int correlationId, ErrorCode error, byte[] payload) {
    public Frame {
        Objects.requireNonNull(error, "error");
        Objects.requireNonNull(payload, "payload");
    }
}
