package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.protocol.Messages;

import java.util.Objects;

final class ClientSupport {
    private ClientSupport() {}

    static Messages.Response requireSuccess(Messages.Reply reply) {
        Objects.requireNonNull(reply, "reply");
        if (reply.error() != ErrorCode.NONE) {
            String message =
                    reply.body() instanceof Messages.ErrorBody errorBody
                            ? errorBody.message()
                            : reply.error().name();
            throw new CourseException(reply.error(), message);
        }
        return reply.body();
    }

    static <T extends Messages.Response> T requireBody(Messages.Reply reply, Class<T> bodyType) {
        Messages.Response body = requireSuccess(reply);
        if (!bodyType.isInstance(body)) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST,
                    "Expected "
                            + bodyType.getSimpleName()
                            + " but received "
                            + body.getClass().getSimpleName());
        }
        return bodyType.cast(body);
    }

    static int recordBytes(io.simplekafka.model.LogRecord record) {
        var data = record.data();
        int keyBytes = data.key() == null ? 0 : data.key().length;
        int stampBytes = data.producerStamp() == null ? 0 : ProducerStamp.ENCODED_OVERHEAD;
        return Math.addExact(32 + stampBytes, Math.addExact(keyBytes, data.value().length));
    }
}
