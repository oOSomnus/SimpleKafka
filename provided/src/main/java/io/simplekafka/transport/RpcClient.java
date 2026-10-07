package io.simplekafka.transport;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Endpoint;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.net.Socket;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

/** One request per fresh TCP connection; operations are never retried implicitly. */
public final class RpcClient implements AutoCloseable {
    private final Endpoint endpoint;
    private final int timeoutMillis;
    private final AtomicInteger correlations = new AtomicInteger();
    private volatile boolean closed;

    public RpcClient(Endpoint endpoint, int timeoutMillis) {
        this.endpoint = Objects.requireNonNull(endpoint);
        if (timeoutMillis <= 0) throw new IllegalArgumentException("timeoutMillis must be positive");
        this.timeoutMillis = timeoutMillis;
    }
    public Endpoint endpoint() { return endpoint; }
    public Messages.Reply call(Messages.Request request) {
        if (closed) throw new IllegalStateException("client is closed");
        int correlation = correlations.incrementAndGet();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            FrameCodec.write(socket.getOutputStream(), new Frame(request.apiId(), correlation, ErrorCode.NONE,
                    MessageCodec.encodeRequest(request)));
            Frame responseFrame = FrameCodec.read(socket.getInputStream());
            if (responseFrame == null) throw new CourseException(ErrorCode.REQUEST_TIMEOUT, "server closed without a response");
            if (responseFrame.api() != request.apiId() || responseFrame.correlationId() != correlation)
                throw new CourseException(ErrorCode.INVALID_REQUEST, "response does not match request");
            Messages.Response body = MessageCodec.decodeReply(responseFrame.api(), responseFrame.error(), responseFrame.payload());
            return new Messages.Reply(responseFrame.error(), body);
        } catch (CourseException exception) {
            throw exception;
        } catch (IOException exception) {
            throw new CourseException(ErrorCode.REQUEST_TIMEOUT, "RPC to " + endpoint + " failed: " + exception.getMessage(), exception);
        }
    }
    @Override public void close() { closed = true; }
}
