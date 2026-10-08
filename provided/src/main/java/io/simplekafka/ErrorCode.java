package io.simplekafka;

import java.util.Arrays;

/** Error codes used by the simpleKafka course protocol, each paired with its wire identifier. */
public enum ErrorCode {
    NONE(0),
    INVALID_REQUEST(1),
    UNKNOWN_TOPIC_OR_PARTITION(2),
    OFFSET_OUT_OF_RANGE(3),
    CORRUPT_RECORD(4),
    NOT_LEADER(5),
    FENCED_EPOCH(6),
    NOT_ENOUGH_REPLICAS(7),
    REQUEST_TIMEOUT(8),
    ILLEGAL_GENERATION(9),
    UNKNOWN_MEMBER(10),
    NOT_ASSIGNED(11),
    NO_ELIGIBLE_LEADER(12),
    STORAGE_ERROR(13);

    private final short wireId;

    ErrorCode(int wireId) {
        this.wireId = (short) wireId;
    }

    /**
     * Returns this code's numeric identifier in the course protocol.
     *
     * @return this error code's wire identifier
     */
    public short wireId() {
        return wireId;
    }

    /**
     * Resolves a course-protocol error identifier.
     *
     * @param wireId numeric error identifier to resolve
     * @return the matching error code
     * @throws CourseException if {@code wireId} is not defined, with error code
     *     {@link ErrorCode#INVALID_REQUEST}
     */
    public static ErrorCode fromWireId(short wireId) {
        return Arrays.stream(values())
                .filter(value -> value.wireId == wireId)
                .findFirst()
                .orElseThrow(
                        () ->
                                new CourseException(
                                        ErrorCode.INVALID_REQUEST,
                                        "Unknown error code: " + wireId));
    }
}
