package io.simplekafka;

import java.util.Objects;

/** A domain failure that carries the error code used to report it through the course protocol. */
public class CourseException extends RuntimeException {
    private final ErrorCode code;

    /**
     * Creates a course failure with a message.
     *
     * @param code protocol error code associated with the failure
     * @param message detail describing the failure
     * @throws NullPointerException if {@code code} is null
     */
    public CourseException(ErrorCode code, String message) {
        super(message);
        this.code = Objects.requireNonNull(code);
    }

    /**
     * Creates a course failure with a message and underlying cause.
     *
     * @param code protocol error code associated with the failure
     * @param message detail describing the failure
     * @param cause underlying cause of the failure
     * @throws NullPointerException if {@code code} is null
     */
    public CourseException(ErrorCode code, String message, Throwable cause) {
        super(message, cause);
        this.code = Objects.requireNonNull(code);
    }

    /**
     * Returns the protocol error code associated with this failure.
     *
     * @return this exception's error code
     */
    public ErrorCode code() {
        return code;
    }
}
