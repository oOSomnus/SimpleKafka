package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
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

    public MetadataRouter(List<Endpoint> bootstrap, RpcClientFactory factory) {
        Objects.requireNonNull(bootstrap, "bootstrap");
        if (bootstrap.isEmpty()) throw new CourseException(ErrorCode.INVALID_REQUEST, "bootstrap list must not be empty");
        try {
            this.bootstrap = List.copyOf(bootstrap);
        } catch (NullPointerException exception) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "bootstrap endpoints must not be null", exception);
        }
        this.factory = Objects.requireNonNull(factory, "factory");
    }

    /** Configured bootstrap endpoints in their fallback order. */
    public List<Endpoint> bootstrap() { return bootstrap; }

    /** Last endpoint that answered a control-plane request, or null before the first such request. */
    public Endpoint activeBootstrap() { return activeBootstrap; }

    /** Refreshes and caches metadata by querying bootstrap endpoints in order. */
    public void refresh(String topic) {
        ensureOpen();
        validateTopic(topic);
        CourseException lastFailure = null;
        for (Endpoint endpoint : bootstrap) {
            RpcClient client;
            try {
                client = clientFor(endpoint);
            } catch (RuntimeException exception) {
                lastFailure = asConnectionFailure(endpoint, exception);
                continue;
            }
            Messages.Reply reply;
            try {
                reply = client.call(new Messages.MetadataRequest(topic));
            } catch (CourseException exception) {
                if (exception.code() != ErrorCode.REQUEST_TIMEOUT) throw exception;
                lastFailure = exception;
                continue;
            } catch (RuntimeException exception) {
                lastFailure = asConnectionFailure(endpoint, exception);
                continue;
            }
            Messages.MetadataBody body = ClientSupport.requireBody(reply, Messages.MetadataBody.class);
            List<PartitionMetadata> fresh = body.partitions();
            for (PartitionMetadata partition : fresh) {
                if (!partition.tp().topic().equals(topic)) {
                    throw new CourseException(ErrorCode.INVALID_REQUEST,
                            "metadata response contains a different topic: " + partition.tp());
                }
            }
            metadataByTopic.put(topic, fresh);
            activeBootstrap = endpoint;
            return;
        }
        if (lastFailure != null) throw lastFailure;
        throw new CourseException(ErrorCode.REQUEST_TIMEOUT, "no bootstrap endpoint could provide metadata");
    }

    /** Returns cached metadata, performing the initial refresh on demand. */
    public List<PartitionMetadata> metadata(String topic) {
        ensureOpen();
        validateTopic(topic);
        List<PartitionMetadata> cached = metadataByTopic.get(topic);
        if (cached == null) {
            refresh(topic);
            cached = metadataByTopic.get(topic);
        }
        if (cached == null) throw new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic: " + topic);
        return cached;
    }

    /** Routes one produce or fetch request using the cached leader and epoch. */
    public Messages.Reply call(TopicPartition tp, Messages.Request request) {
        ensureOpen();
        Objects.requireNonNull(tp, "tp");
        Objects.requireNonNull(request, "request");
        if (request instanceof Messages.ProduceRequest produce && !tp.equals(produce.tp())
                || request instanceof Messages.FetchRequest fetch && !tp.equals(fetch.tp())) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "request topic-partition does not match route");
        }
        if (!(request instanceof Messages.ProduceRequest) && !(request instanceof Messages.FetchRequest)) {
            throw new CourseException(ErrorCode.INVALID_REQUEST, "only produce and fetch use data routing");
        }
        PartitionMetadata route = findMetadata(tp);
        Messages.Reply reply = clientFor(route.leader()).call(request);
        if (reply.error() == ErrorCode.NOT_LEADER || reply.error() == ErrorCode.FENCED_EPOCH) {
            try {
                refresh(tp.topic());
            } catch (RuntimeException ignored) {
                // Return the original business response; the caller must decide whether to issue a new request.
            }
        }
        return reply;
    }

    /** Sends one control request to the active bootstrap; failed requests are never replayed. */
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
            throw new CourseException(ErrorCode.REQUEST_TIMEOUT, "no bootstrap endpoint is available");
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

    private PartitionMetadata findMetadata(TopicPartition tp) {
        List<PartitionMetadata> metadata = metadata(tp.topic());
        for (PartitionMetadata partition : metadata) if (partition.tp().equals(tp)) return partition;
        throw new CourseException(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "unknown topic-partition: " + tp);
    }

    private RpcClient clientFor(Endpoint endpoint) {
        ensureOpen();
        return clients.computeIfAbsent(endpoint, key -> Objects.requireNonNull(factory.connect(key), "factory returned null"));
    }

    private List<Endpoint> controlCandidates() {
        Endpoint preferred = activeBootstrap;
        if (preferred == null || bootstrap.size() == 1 || bootstrap.get(0).equals(preferred)) return bootstrap;
        ArrayList<Endpoint> candidates = new ArrayList<>(bootstrap.size());
        candidates.add(preferred);
        for (Endpoint endpoint : bootstrap) if (!endpoint.equals(preferred)) candidates.add(endpoint);
        return candidates;
    }
    private void rotateBootstrap(List<Endpoint> candidates, int failedIndex) {
        if (candidates.size() > 1) activeBootstrap = candidates.get((failedIndex + 1) % candidates.size());
    }

    private static CourseException asConnectionFailure(Endpoint endpoint, RuntimeException exception) {
        if (exception instanceof CourseException courseException) return courseException;
        return new CourseException(ErrorCode.REQUEST_TIMEOUT,
                "could not connect to bootstrap endpoint " + endpoint + ": " + exception.getMessage(), exception);
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

    @Override public void close() {
        if (!closed.compareAndSet(false, true)) return;
        Set<RpcClient> uniqueClients = new HashSet<>(clients.values());
        clients.clear();
        metadataByTopic.clear();
        uniqueClients.forEach(RpcClient::close);
    }
}
