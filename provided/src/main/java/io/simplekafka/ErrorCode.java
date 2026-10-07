package io.simplekafka;

import java.util.Arrays;

public enum ErrorCode {
    NONE(0), INVALID_REQUEST(1), UNKNOWN_TOPIC_OR_PARTITION(2), OFFSET_OUT_OF_RANGE(3),
    CORRUPT_RECORD(4), NOT_LEADER(5), FENCED_EPOCH(6), NOT_ENOUGH_REPLICAS(7),
    REQUEST_TIMEOUT(8), ILLEGAL_GENERATION(9), UNKNOWN_MEMBER(10), NOT_ASSIGNED(11),
    NO_ELIGIBLE_LEADER(12), STORAGE_ERROR(13);

    private final short wireId;
    ErrorCode(int wireId) { this.wireId = (short) wireId; }
    public short wireId() { return wireId; }
    public static ErrorCode fromWireId(short wireId) {
        return Arrays.stream(values()).filter(value -> value.wireId == wireId).findFirst()
                .orElseThrow(() -> new CourseException(ErrorCode.INVALID_REQUEST, "Unknown error code: " + wireId));
    }
}
