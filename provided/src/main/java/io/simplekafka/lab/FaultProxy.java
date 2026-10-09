package io.simplekafka.lab;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Endpoint;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.protocol.Messages;

import java.io.IOException;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.ServerSocket;
import java.net.Socket;
import java.net.SocketException;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/** A loopback framed-TCP proxy with one-shot, explicitly armed request/reply faults. */
public final class FaultProxy implements AutoCloseable {
    private static final int CONNECT_TIMEOUT_MILLIS = 2_000;
    private static final int READ_TIMEOUT_MILLIS = 10_000;

    private final Endpoint upstream;
    private final AtomicBoolean closed = new AtomicBoolean();
    private final Set<Socket> downstreamSockets = ConcurrentHashMap.newKeySet();
    private final Object socketsLock = new Object();
    private final Set<Socket> upstreamSockets = ConcurrentHashMap.newKeySet();
    private final ConcurrentMap<Short, AtomicInteger> forwarded = new ConcurrentHashMap<>();
    private final ConcurrentMap<Short, AtomicInteger> dropped = new ConcurrentHashMap<>();
    private final ConcurrentMap<Short, AtomicInteger> rejected = new ConcurrentHashMap<>();
    private final ExecutorService workers =
            Executors.newCachedThreadPool(
                    task -> {
                        Thread worker = new Thread(task, "simple-kafka-fault-proxy-worker");
                        worker.setDaemon(true);
                        return worker;
                    });

    private ServerSocket server;
    private Thread acceptor;
    private Endpoint endpoint;
    private Fault fault;

    /** Creates a proxy that will forward requests to {@code upstream}. */
    public FaultProxy(Endpoint upstream) {
        this.upstream = Objects.requireNonNull(upstream, "upstream");
    }

    /** Returns the real broker endpoint this proxy forwards to. */
    public Endpoint upstream() {
        return upstream;
    }

    /** Starts listening on an OS-assigned loopback port and returns the proxy endpoint. */
    public synchronized Endpoint start() {
        ensureOpen();
        if (server != null) return endpoint;
        ServerSocket listener = null;
        try {
            listener = new ServerSocket();
            listener.bind(new InetSocketAddress(InetAddress.getByName("127.0.0.1"), 0));
            server = listener;
            endpoint = new Endpoint("127.0.0.1", listener.getLocalPort());
            acceptor = new Thread(this::acceptLoop, "simple-kafka-fault-proxy-acceptor");
            acceptor.setDaemon(true);
            acceptor.start();
            return endpoint;
        } catch (IOException exception) {
            if (listener != null) {
                try {
                    listener.close();
                } catch (IOException closeFailure) {
                    exception.addSuppressed(closeFailure);
                }
            }
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR, "cannot start fault proxy", exception);
        }
    }

    /** Returns the bound proxy endpoint. */
    public synchronized Endpoint endpoint() {
        if (server == null) throw new IllegalStateException("fault proxy has not started");
        return endpoint;
    }

    /** Returns whether a one-shot fault is waiting for its matching API request. */
    public synchronized boolean armed() {
        return fault != null;
    }

    /** Drops the reply to the next matching request, after the upstream handles it successfully. */
    public synchronized void dropNextReply(short api) {
        arm(api, FaultKind.DROP_REPLY);
    }

    /** Rejects the next matching request before it is forwarded upstream. */
    public synchronized void rejectNextRequest(short api) {
        arm(api, FaultKind.REJECT_REQUEST);
    }

    /** Returns the number of requests actually written to the upstream for {@code api}. */
    public int forwarded(short api) {
        return count(forwarded, api);
    }

    /** Returns the number of successful upstream replies deliberately dropped for {@code api}. */
    public int dropped(short api) {
        return count(dropped, api);
    }

    /** Returns the number of matching requests rejected before forwarding for {@code api}. */
    public int rejected(short api) {
        return count(rejected, api);
    }

    private synchronized void arm(short api, FaultKind kind) {
        ensureOpen();
        Api.requireKnown(api);
        if (fault != null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "a fault is already armed");
        fault = new Fault(api, kind);
    }

    private void acceptLoop() {
        while (!closed.get()) {
            try {
                Socket socket = server.accept();
                if (closed.get()) {
                    socket.close();
                    continue;
                }
                if (!register(downstreamSockets, socket)) {
                    socket.close();
                    continue;
                }
                workers.execute(() -> serve(socket));
            } catch (SocketException exception) {
                if (!closed.get()) close();
            } catch (IOException | RuntimeException exception) {
                if (!closed.get()) close();
            }
        }
    }

    private void serve(Socket downstream) {
        try (downstream) {
            downstream.setSoTimeout(READ_TIMEOUT_MILLIS);
            while (!closed.get()) {
                Frame request = FrameCodec.read(downstream.getInputStream());
                if (request == null) return;
                Fault matched = takeMatchingFault(request.api());
                if (matched != null && matched.kind == FaultKind.REJECT_REQUEST) {
                    Frame response =
                            new Frame(
                                    request.api(),
                                    request.correlationId(),
                                    ErrorCode.REQUEST_TIMEOUT,
                                    MessageCodec.encodeReply(
                                            new Messages.ErrorBody(
                                                    "injected pre-forward failure")));
                    increment(rejected, request.api());
                    FrameCodec.write(downstream.getOutputStream(), response);
                    downstream.getOutputStream().flush();
                    continue;
                }

                Frame response = forward(request);
                if (response == null) return;
                if (matched != null
                        && matched.kind == FaultKind.DROP_REPLY
                        && response.error() == ErrorCode.NONE) {
                    increment(dropped, request.api());
                    return;
                }
                FrameCodec.write(downstream.getOutputStream(), response);
                downstream.getOutputStream().flush();
            }
        } catch (IOException | RuntimeException ignored) {
            // A broken client or upstream affects only this connection.
        } finally {
            unregister(downstreamSockets, downstream);
        }
    }

    private Frame forward(Frame request) throws IOException {
        try (Socket socket = new Socket()) {
            if (!register(upstreamSockets, socket))
                throw new SocketException("fault proxy is closed");
            try {
                socket.connect(
                        new InetSocketAddress(upstream.host(), upstream.port()),
                        CONNECT_TIMEOUT_MILLIS);
                socket.setSoTimeout(READ_TIMEOUT_MILLIS);
                FrameCodec.write(socket.getOutputStream(), request);
                socket.getOutputStream().flush();
                increment(forwarded, request.api());
                return FrameCodec.read(socket.getInputStream());
            } finally {
                try {
                    socket.close();
                } finally {
                    unregister(upstreamSockets, socket);
                }
            }
        }
    }

    private boolean register(Set<Socket> sockets, Socket socket) {
        synchronized (socketsLock) {
            if (closed.get()) return false;
            sockets.add(socket);
            return true;
        }
    }

    private void unregister(Set<Socket> sockets, Socket socket) {
        synchronized (socketsLock) {
            sockets.remove(socket);
        }
    }

    private synchronized Fault takeMatchingFault(short api) {
        if (fault == null || fault.api != api) return null;
        Fault matched = fault;
        fault = null;
        return matched;
    }

    private static int count(ConcurrentMap<Short, AtomicInteger> counts, short api) {
        AtomicInteger value = counts.get(api);
        return value == null ? 0 : value.get();
    }

    private static void increment(ConcurrentMap<Short, AtomicInteger> counts, short api) {
        counts.computeIfAbsent(api, ignored -> new AtomicInteger()).incrementAndGet();
    }

    private void ensureOpen() {
        if (closed.get()) throw new IllegalStateException("fault proxy is closed");
    }

    /** Stops accepting requests and closes every active downstream and upstream socket. */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        ServerSocket listener;
        synchronized (this) {
            listener = server;
            fault = null;
        }
        if (listener != null) {
            try {
                listener.close();
            } catch (IOException ignored) {
            }
        }
        synchronized (socketsLock) {
            closeSockets(downstreamSockets);
            closeSockets(upstreamSockets);
        }
        workers.shutdownNow();
        Thread thread = acceptor;
        if (thread != null && thread != Thread.currentThread()) {
            try {
                thread.join(2_000);
            } catch (InterruptedException exception) {
                Thread.currentThread().interrupt();
            }
        }
        try {
            workers.awaitTermination(2, TimeUnit.SECONDS);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
        }
    }

    private static void closeSockets(Set<Socket> sockets) {
        for (Socket socket : sockets) {
            try {
                socket.close();
            } catch (IOException ignored) {
            }
        }
    }

    private enum FaultKind {
        DROP_REPLY,
        REJECT_REQUEST
    }

    private record Fault(short api, FaultKind kind) {}
}
