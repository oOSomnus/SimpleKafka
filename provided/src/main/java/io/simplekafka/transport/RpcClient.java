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

    /**
     * Creates a client pinned to an endpoint with a connect and read timeout.
     *
     * @param endpoint server endpoint to use for every call
     * @param timeoutMillis connect and socket-read timeout in milliseconds
     * @throws NullPointerException if {@code endpoint} is {@code null}
     * @throws IllegalArgumentException if {@code timeoutMillis} is not positive
     */
    public RpcClient(Endpoint endpoint, int timeoutMillis) {
        this.endpoint = Objects.requireNonNull(endpoint);
        if (timeoutMillis <= 0)
            throw new IllegalArgumentException("timeoutMillis must be positive");
        this.timeoutMillis = timeoutMillis;
    }

    /**
     * Returns the endpoint to which this client is pinned.
     *
     * @return configured server endpoint
     */
    public Endpoint endpoint() {
        return endpoint;
    }

    /**
     * Sends one request over a fresh TCP connection without retrying it.
     *
     * <p>The reply's error code is returned in the {@link Messages.Reply}; a broker error is not
     * thrown by this method.
     *
     * @param request request to encode and send
     * @return decoded reply, including any broker error code
     * @throws NullPointerException if {@code request} is {@code null}
     * @throws IllegalStateException if this client has been closed
     * @throws CourseException with {@code INVALID_REQUEST} for a mismatched or malformed response,
     *     or with {@code REQUEST_TIMEOUT} for an I/O failure or missing response
     */
    public Messages.Reply call(Messages.Request request) {
        if (closed) throw new IllegalStateException("client is closed");
        int correlation = correlations.incrementAndGet();
        try (Socket socket = new Socket()) {
            socket.connect(new InetSocketAddress(endpoint.host(), endpoint.port()), timeoutMillis);
            socket.setSoTimeout(timeoutMillis);
            FrameCodec.write(
                    socket.getOutputStream(),
                    new Frame(
                            request.apiId(),
                            correlation,
                            ErrorCode.NONE,
                            MessageCodec.encodeRequest(request)));
            Frame responseFrame = FrameCodec.read(socket.getInputStream());
            if (responseFrame == null)
                throw new CourseException(
                        ErrorCode.REQUEST_TIMEOUT, "server closed without a response");
            if (responseFrame.api() != request.apiId()
                    || responseFrame.correlationId() != correlation)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "response does not match request");
            Messages.Response body =
                    MessageCodec.decodeReply(
                            responseFrame.api(), responseFrame.error(), responseFrame.payload());
            return new Messages.Reply(responseFrame.error(), body);
        } catch (CourseException exception) {
            throw exception;
        } catch (IOException exception) {
            throw new CourseException(
                    ErrorCode.REQUEST_TIMEOUT,
                    "RPC to " + endpoint + " failed: " + exception.getMessage(),
                    exception);
        }
    }

    /**
     * Marks this client closed so subsequent calls are rejected.
     *
     * <p>Closing is idempotent and does not cancel an in-progress call; the client retains no
     * socket between calls.
     */
    @Override
    public void close() {
        closed = true;
    }
}
