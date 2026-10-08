package io.simplekafka.model;

import java.nio.charset.StandardCharsets;
import java.util.Objects;

public record GroupToken(String group, String member, int generation) {
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
