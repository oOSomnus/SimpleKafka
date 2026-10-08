package io.simplekafka.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

/**
 * A group membership token used to fence operations against stale generations.
 *
 * <p>Group and member identifiers are limited by UTF-8 byte length, not by the topic identifier's
 * ASCII character set.
 *
 * @param group non-null group identifier of 1 to 255 UTF-8 bytes
 * @param member non-null member identifier of 1 to 255 UTF-8 bytes
 * @param generation nonnegative group generation
 */
public record GroupToken(String group, String member, int generation) {
    /**
     * Creates a token after validating its identifiers and generation.
     *
     * @param group group identifier
     * @param member member identifier
     * @param generation group generation
     * @throws NullPointerException if {@code group} or {@code member} is null
     * @throws IllegalArgumentException if either identifier is empty or longer than 255 UTF-8
     *     bytes, or if {@code generation} is negative
     */
    public GroupToken {
        validate(group, "group");
        validate(member, "member");
        if (generation < 0) throw new IllegalArgumentException("generation must be nonnegative");
    }

    private static void validate(String value, String field) {
        Objects.requireNonNull(value, field);
        int length = value.getBytes(StandardCharsets.UTF_8).length;
        if (length == 0 || length > 255)
            throw new IllegalArgumentException(field + " must contain 1..255 UTF-8 bytes");
    }
}
