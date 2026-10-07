package io.simplekafka.model;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;

public enum Acks {
    LEADER((short) 1), ALL((short) -1);
    private final short wireId;
    Acks(short wireId) { this.wireId = wireId; }
    public short wireId() { return wireId; }
    public static Acks fromWireId(short id) {
        for (Acks acks : values()) if (acks.wireId == id) return acks;
        throw new CourseException(ErrorCode.INVALID_REQUEST, "unsupported acks value: " + id);
    }
}
