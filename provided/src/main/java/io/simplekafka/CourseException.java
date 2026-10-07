package io.simplekafka;

import java.util.Objects;

public class CourseException extends RuntimeException {
    private final ErrorCode code;
    public CourseException(ErrorCode code, String message) { super(message); this.code = Objects.requireNonNull(code); }
    public CourseException(ErrorCode code, String message, Throwable cause) { super(message, cause); this.code = Objects.requireNonNull(code); }
    public ErrorCode code() { return code; }
}
