package io.simplekafka.protocol;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;

/** Fixed API identifiers for the simpleKafka course teaching protocol. */
public final class Api {
    public static final short METADATA = 1,
            PRODUCE = 2,
            FETCH = 3,
            COMMIT_OFFSET = 4,
            FETCH_OFFSET = 5,
            JOIN_GROUP = 6,
            HEARTBEAT = 7,
            LEAVE_GROUP = 8,
            GROUP_ASSIGNMENT = 9,
            REPLICA_FETCH = 10;

    private Api() {}

    /**
     * Tests whether an identifier is one of the defined course API ids.
     *
     * @param id API identifier to test
     * @return {@code true} when {@code id} is between {@link #METADATA} and {@link #REPLICA_FETCH},
     *     inclusive
     */
    public static boolean known(short id) {
        return id >= METADATA && id <= REPLICA_FETCH;
    }

    /**
     * Requires an identifier to be one of the defined course API ids.
     *
     * @param id API identifier to validate
     * @throws CourseException if {@code id} is unknown, with error code
     *     {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     */
    public static void requireKnown(short id) {
        if (!known(id)) throw new CourseException(ErrorCode.INVALID_REQUEST, "unknown API: " + id);
    }
}
