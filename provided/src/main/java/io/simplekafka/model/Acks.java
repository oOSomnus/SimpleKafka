package io.simplekafka.model;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;

/**
 * Acknowledgment mode supported by the course producer API.
 *
 * <p>{@link #LEADER} is wire value {@code 1} and may return after the leader append completes.
 * {@link #ALL} is wire value {@code -1}; it requires sufficient in-sync replicas and waits for the
 * high watermark to cover the append. A timeout does not establish whether an append occurred.
 */
public enum Acks {
    LEADER((short) 1),
    ALL((short) -1);
    private final short wireId;

    Acks(short wireId) {
        this.wireId = wireId;
    }

    /**
     * Returns this acknowledgment mode's course-protocol wire value.
     *
     * @return {@code 1} for {@link #LEADER} or {@code -1} for {@link #ALL}
     */
    public short wireId() {
        return wireId;
    }

    /**
     * Resolves a supported acknowledgment mode from its course-protocol wire value.
     *
     * @param id wire value to resolve
     * @return the matching acknowledgment mode
     * @throws CourseException if {@code id} is not supported, with error code
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     */
    public static Acks fromWireId(short id) {
        for (Acks acks : values()) if (acks.wireId == id) return acks;
        throw new CourseException(ErrorCode.INVALID_REQUEST, "unsupported acks value: " + id);
    }
}
