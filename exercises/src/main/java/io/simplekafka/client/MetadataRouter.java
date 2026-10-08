package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;
import io.simplekafka.transport.RpcClientFactory;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicBoolean;

/** Routes data requests to cached leaders and control requests to a surviving bootstrap broker. */
public final class MetadataRouter implements AutoCloseable {
    private final List<Endpoint> bootstrap;
    private final RpcClientFactory factory;
    private final Map<Endpoint, RpcClient> clients = new ConcurrentHashMap<>();
    private final Map<String, List<PartitionMetadata>> metadataByTopic = new ConcurrentHashMap<>();
    private final AtomicBoolean closed = new AtomicBoolean();
    private volatile Endpoint activeBootstrap;

    /**
     * Configures bootstrap endpoints in fallback order; RPC clients are created lazily and cached
     * per endpoint.
     *
     * @param bootstrap non-empty ordered bootstrap endpoint list
     * @param factory factory used to create clients on demand
     * @throws NullPointerException if {@code bootstrap} or {@code factory} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} if the list is empty or
     *     contains a null endpoint
     */
    public MetadataRouter(List<Endpoint> bootstrap, RpcClientFactory factory) {
        Objects.requireNonNull(bootstrap, "bootstrap");
        if (bootstrap.isEmpty())
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "bootstrap list must not be empty");
        try {
            this.bootstrap = List.copyOf(bootstrap);
        } catch (NullPointerException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "bootstrap endpoints must not be null", exception);
        }
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    /**
     * Returns the configured bootstrap endpoints in their fallback order.
     *
     * @return the immutable configured endpoint list
     */
    public List<Endpoint> bootstrap() {
        return bootstrap;
    }

    /**
     * Returns the currently preferred control-plane endpoint, or null before one is selected.
     *
     * @return the active bootstrap endpoint, or {@code null} before the first selection
     */
    public Endpoint activeBootstrap() {
        return activeBootstrap;
    }

    /**
     * Step 27: refresh this topic's leader/epoch metadata through bootstrap endpoints in configured
     * order. Only a connection failure or {@link ErrorCode#REQUEST_TIMEOUT} falls through; a
     * successful response is cached and selects its endpoint as active. Test: Step27Test. Lesson:
     * docs/book/chapters/07-failover.tex, Step 27.
     *
     * @param topic topic whose metadata should be refreshed
     * @throws IllegalStateException if this router is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an invalid topic or
     *     mismatched response topic, or with the failure from the last attempted bootstrap if every
     *     attempt fails
     * @throws ExerciseNotImplementedException while the Step 27 exercise method is a skeleton
     */
    public void refresh(String topic) {
        throw new ExerciseNotImplementedException(27, "refresh");
    }

    /**
     * Returns cached metadata, performing the initial refresh on demand.
     *
     * @param topic topic whose metadata is requested
     * @return the cached partition metadata
     * @throws IllegalStateException if this router is closed
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an invalid topic, or with
     *     {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if no metadata is available for the topic
     * @throws ExerciseNotImplementedException while the delegated Step 27 refresh method is a
     *     skeleton
     */
    public List<PartitionMetadata> metadata(String topic) {
        ensureOpen();
        validateTopic(topic);
        List<PartitionMetadata> cached = metadataByTopic.get(topic);
        if (cached == null) {
            refresh(topic);
            cached = metadataByTopic.get(topic);
        }
        if (cached == null)
            throw new CourseException(
                    ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic: " + topic);
        return cached;
    }

    /**
     * Step 27: route a produce/fetch request for {@code tp} by cached leader and epoch without
     * business retries. On {@link ErrorCode#NOT_LEADER} or {@link ErrorCode#FENCED_EPOCH}, refresh
     * best-effort but return the original reply unchanged. Test: Step27Test. Lesson:
     * docs/book/chapters/07-failover.tex, Step 27.
     *
     * @param tp partition used to select the cached leader
     * @param request produce or fetch request whose partition must match {@code tp}
     * @return the routed request's original reply
     * @throws IllegalStateException if this router is closed
     * @throws NullPointerException if {@code tp} or {@code request} is null
     * @throws CourseException with {@link ErrorCode#INVALID_REQUEST} for an unsupported request or
     *     partition mismatch, or with {@link ErrorCode#UNKNOWN_TOPIC_OR_PARTITION} if metadata is
     *     unavailable for the partition
     * @throws ExerciseNotImplementedException while the Step 27 exercise method is a skeleton
     */
    public Messages.Reply call(TopicPartition tp, Messages.Request request) {
        throw new ExerciseNotImplementedException(27, "call");
    }

    /**
     * Sends one control request to the active bootstrap, or configured endpoints in order before
     * one is active. A failed request may rotate the preferred endpoint but is never replayed.
     *
     * @param request control request to send
     * @return the broker's reply
     * @throws IllegalStateException if this router is closed
     * @throws NullPointerException if {@code request} is null
     * @throws CourseException if client creation or the RPC call fails, or no bootstrap is
     *     available
     */
    public Messages.Reply controlCall(Messages.Request request) {
        ensureOpen();
        Objects.requireNonNull(request, "request");
        List<Endpoint> candidates = controlCandidates();
        CourseException lastFailure = null;
        RpcClient client = null;
        int clientIndex = 0;
        for (; clientIndex < candidates.size(); clientIndex++) {
            Endpoint endpoint = candidates.get(clientIndex);
            try {
                client = clientFor(endpoint);
                break;
            } catch (RuntimeException exception) {
                lastFailure = asConnectionFailure(endpoint, exception);
            }
        }
        if (client == null) {
            if (lastFailure != null) throw lastFailure;
            throw new CourseException(
                    ErrorCode.REQUEST_TIMEOUT, "no bootstrap endpoint is available");
        }
        Endpoint endpoint = candidates.get(clientIndex);
        try {
            Messages.Reply reply = client.call(request);
            activeBootstrap = endpoint;
            return reply;
        } catch (RuntimeException failure) {
            rotateBootstrap(candidates, clientIndex);
            throw failure;
        }
    }

    private RpcClient clientFor(Endpoint endpoint) {
        ensureOpen();
        return clients.computeIfAbsent(
                endpoint,
                key -> Objects.requireNonNull(factory.connect(key), "factory returned null"));
    }

    private List<Endpoint> controlCandidates() {
        Endpoint preferred = activeBootstrap;
        if (preferred == null || bootstrap.size() == 1 || bootstrap.get(0).equals(preferred))
            return bootstrap;
        ArrayList<Endpoint> candidates = new ArrayList<>(bootstrap.size());
        candidates.add(preferred);
        for (Endpoint endpoint : bootstrap)
            if (!endpoint.equals(preferred)) candidates.add(endpoint);
        return candidates;
    }

    private void rotateBootstrap(List<Endpoint> candidates, int failedIndex) {
        if (candidates.size() > 1)
            activeBootstrap = candidates.get((failedIndex + 1) % candidates.size());
    }

    private static CourseException asConnectionFailure(
            Endpoint endpoint, RuntimeException exception) {
        if (exception instanceof CourseException courseException) return courseException;
        return new CourseException(
                ErrorCode.REQUEST_TIMEOUT,
                "could not connect to bootstrap endpoint "
                        + endpoint
                        + ": "
                        + exception.getMessage(),
                exception);
    }

    private static void validateTopic(String topic) {
        try {
            new TopicPartition(topic, 0);
        } catch (RuntimeException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "invalid topic", exception);
        }
    }

    private void ensureOpen() {
        if (closed.get()) throw new IllegalStateException("metadata router is closed");
    }

    /**
     * Closes each unique cached RPC client once and clears client and metadata caches; repeated
     * calls have no effect. Subsequent routed operations fail with {@link IllegalStateException}.
     */
    @Override
    public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Set<RpcClient> uniqueClients = new HashSet<>(clients.values());
        clients.clear();
        metadataByTopic.clear();
        uniqueClients.forEach(RpcClient::close);
    }
}
