package io.simplekafka.protocol;

import io.simplekafka.ErrorCode;

import java.util.Objects;

/**
 * A frame value used by the course teaching protocol.
 *
 * <p>Payload ownership transfers to the frame at construction without copying. This value checks
 * that {@code error} and {@code payload} are non-null; API validity and payload size are handled by
 * the frame codec.
 *
 * @param api course API identifier
 * @param correlationId request correlation identifier, echoed in the response frame
 * @param error error code carried by the frame; request frames use {@link ErrorCode#NONE}
 * @param payload frame body bytes, retained without copying
 */
public record Frame(short api, int correlationId, ErrorCode error, byte[] payload) {
    /**
     * Creates a frame while retaining the supplied payload array without copying it.
     *
     * @param api course API identifier
     * @param correlationId request correlation identifier
     * @param error error code carried by the frame
     * @param payload frame body bytes retained by this value
     * @throws NullPointerException if {@code error} or {@code payload} is {@code null}
     */
    public Frame {
        Objects.requireNonNull(error, "error");
        Objects.requireNonNull(payload, "payload");
    }
}
