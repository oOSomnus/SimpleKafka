package io.simplekafka.transport;

import io.simplekafka.CourseException;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Endpoint;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;
import java.io.IOException;
import java.net.InetAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Function;

/** Loopback-only framed TCP server with one worker per connection. */
public final class BrokerServer implements AutoCloseable {
    private final String host;
    private final int requestedPort;
    private final Function<Messages.Request, Messages.Reply> handler;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<Socket> connections = ConcurrentHashMap.newKeySet();
    private final ExecutorService workers = Executors.newCachedThreadPool(r -> {
        Thread thread = new Thread(r, "simple-kafka-connection"); thread.setDaemon(true); return thread;
    });
    private ServerSocket server;
    private Thread acceptor;
    public BrokerServer(String host, int port, Function<Messages.Request, Messages.Reply> handler) {
        this.host = Objects.requireNonNull(host);
        this.requestedPort = port;
        this.handler = Objects.requireNonNull(handler);
        if (!(host.equals("127.0.0.1") || host.equals("localhost")) || port < 0 || port > 65535)
            throw new IllegalArgumentException("brokers must bind to loopback and a valid port");
    }
    public synchronized Endpoint start() {
        if (closed.get()) throw new IllegalStateException("broker is closed");
        if (server != null) return endpoint();
        try {
            server = new ServerSocket(requestedPort, 50, InetAddress.getByName(host));
            acceptor = new Thread(this::acceptLoop, "simple-kafka-accept-" + server.getLocalPort());
            acceptor.setDaemon(true); acceptor.start();
            return endpoint();
        } catch (IOException exception) {
            throw new CourseException(ErrorCode.STORAGE_ERROR, "cannot bind broker", exception);
        }
    }
    public synchronized Endpoint endpoint() {
        if (server == null) throw new IllegalStateException("broker has not started");
        return new Endpoint("127.0.0.1", server.getLocalPort());
    }
    private void acceptLoop() {
        while (!closed.get()) {
            try {
                Socket socket = server.accept();
                if (closed.get()) { socket.close(); continue; }
                connections.add(socket);
                try {
                    workers.execute(() -> serve(socket));
                } catch (RuntimeException exception) {
                    connections.remove(socket);
                    socket.close();
                    if (!closed.get()) throw exception;
                }
            } catch (SocketException exception) {
                if (!closed.get()) throw new CourseException(ErrorCode.STORAGE_ERROR, "broker accept failed", exception);
            } catch (IOException exception) {
                if (!closed.get()) throw new CourseException(ErrorCode.STORAGE_ERROR, "broker accept failed", exception);
            }
        }
    }
    private void serve(Socket socket) {
        try (socket) {
            socket.setSoTimeout(30000);
            while (!closed.get()) {
                Frame requestFrame = FrameCodec.read(socket.getInputStream());
                if (requestFrame == null) return;
                Messages.Reply reply;
                try {
                    if (requestFrame.error() != ErrorCode.NONE) throw new CourseException(ErrorCode.INVALID_REQUEST, "request error field must be zero");
                    Api.requireKnown(requestFrame.api());
                    Messages.Request request = MessageCodec.decodeRequest(requestFrame.api(), requestFrame.payload());
                    reply = handler.apply(MessageCodec.capFetchBudget(request));
                } catch (CourseException exception) {
                    reply = Messages.Reply.failure(exception.code(), exception.getMessage() == null ? exception.code().name() : exception.getMessage());
                } catch (ExerciseNotImplementedException exception) {
                    reply = Messages.Reply.failure(ErrorCode.STORAGE_ERROR, exception.getMessage());
                } catch (RuntimeException exception) {
                    reply = Messages.Reply.failure(ErrorCode.STORAGE_ERROR, "broker request failed");
                }
                Frame response = encodeResponse(requestFrame, reply);
                FrameCodec.write(socket.getOutputStream(), response);
                socket.getOutputStream().flush();
            }
        } catch (IOException | RuntimeException ignored) {
            // A malformed/abrupt client closes only its own connection.
        } finally {
            connections.remove(socket);
        }
    }

    private static Frame encodeResponse(Frame requestFrame, Messages.Reply reply) {
        byte[] payload;
        ErrorCode error = reply.error();
        try {
            payload = MessageCodec.encodeReply(reply.body());
        } catch (CourseException exception) {
            Messages.Reply fallback =
                    Messages.Reply.failure(ErrorCode.INVALID_REQUEST, "broker reply could not be encoded");
            error = fallback.error();
            payload = MessageCodec.encodeReply(fallback.body());
        }
        return new Frame(requestFrame.api(), requestFrame.correlationId(), error, payload);
    }

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        try { if (server != null) server.close(); } catch (IOException ignored) { }
        for (Socket socket : connections) {
            try { socket.close(); } catch (IOException ignored) { }
        }
        workers.shutdownNow();
        if (acceptor != null) {
            try { acceptor.join(2000); } catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
        }
        try { workers.awaitTermination(2, TimeUnit.SECONDS); }
        catch (InterruptedException exception) { Thread.currentThread().interrupt(); }
    }
}
