package io.simplekafka.protocol;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;

public final class Api {
    public static final short METADATA = 1, PRODUCE = 2, FETCH = 3, COMMIT_OFFSET = 4,
            FETCH_OFFSET = 5, JOIN_GROUP = 6, HEARTBEAT = 7, LEAVE_GROUP = 8,
            GROUP_ASSIGNMENT = 9, REPLICA_FETCH = 10;
    private Api() {}
    public static boolean known(short id) { return id >= METADATA && id <= REPLICA_FETCH; }
    public static void requireKnown(short id) {
        if (!known(id)) throw new CourseException(ErrorCode.INVALID_REQUEST, "unknown API: " + id);
    }
}
