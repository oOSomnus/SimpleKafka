package io.simplekafka.model;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;

/** Stable application-level identity and signed account delta for one business effect. */
public record BusinessEvent(String eventId, String account, long delta) {
    public BusinessEvent {
        requireIdentifier(eventId, "eventId");
        requireIdentifier(account, "account");
    }

    private static void requireIdentifier(String value, String field) {
        if (value == null || value.isEmpty())
            throw new IllegalArgumentException(field + " must be nonempty");
        try {
            int bytes =
                    StandardCharsets.UTF_8
                            .newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .encode(CharBuffer.wrap(value))
                            .remaining();
            if (bytes > 255) throw new IllegalArgumentException(field + " exceeds 255 UTF-8 bytes");
        } catch (CharacterCodingException exception) {
            throw new IllegalArgumentException(field + " is not valid UTF-8", exception);
        }
    }
}
