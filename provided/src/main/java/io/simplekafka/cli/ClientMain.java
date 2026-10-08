package io.simplekafka.cli;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.Acks;
import io.simplekafka.model.Endpoint;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.protocol.Messages;
import io.simplekafka.transport.RpcClient;

import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Minimal command-line producer and fetcher using the course RPC protocol directly. */
public final class ClientMain {
    private ClientMain() {}

    public static void main(String[] args) {
        Arguments arguments = Arguments.parse(args);
        Endpoint bootstrap = endpoint(arguments.required("--bootstrap"));
        String topic = arguments.required("--topic");
        int partition = integer(arguments.required("--partition"), "partition");
        if (partition < 0) throw invalid("partition must be nonnegative");
        TopicPartition tp = new TopicPartition(topic, partition);
        try (RpcClient client = new RpcClient(bootstrap, 5000)) {
            PartitionMetadata metadata = metadata(client, topic, tp);
            switch (arguments.command()) {
                case "produce" -> produce(client, arguments, tp, metadata.epoch());
                case "fetch" -> fetch(client, arguments, tp, metadata.epoch());
                default -> throw invalid("command must be produce or fetch");
            }
        }
    }

    private static void produce(
            RpcClient client, Arguments arguments, TopicPartition tp, int epoch) {
        byte[] key =
                arguments.optional("--key") == null
                        ? null
                        : arguments.optional("--key").getBytes(StandardCharsets.UTF_8);
        byte[] value = arguments.required("--value").getBytes(StandardCharsets.UTF_8);
        long timestamp = System.currentTimeMillis();
        Messages.ProduceRequest request =
                new Messages.ProduceRequest(
                        tp,
                        epoch,
                        Acks.LEADER,
                        5000,
                        List.of(new RecordData(key, value, timestamp)));
        Messages.ProduceBody body = expect(client.call(request), Messages.ProduceBody.class);
        System.out.printf(
                "firstOffset=%d nextOffset=%d%n",
                body.result().firstOffset(), body.result().nextOffset());
    }

    private static void fetch(RpcClient client, Arguments arguments, TopicPartition tp, int epoch) {
        long offset = longInteger(arguments.valueOrDefault("--offset", "0"), "offset");
        Messages.FetchRequest request =
                new Messages.FetchRequest(tp, epoch, offset, 100, 262144, null);
        Messages.FetchBody body = expect(client.call(request), Messages.FetchBody.class);
        for (LogRecord record : body.records()) {
            System.out.printf(
                    "offset=%d value=%s%n",
                    record.offset(), new String(record.data().value(), StandardCharsets.UTF_8));
        }
    }

    private static PartitionMetadata metadata(RpcClient client, String topic, TopicPartition tp) {
        Messages.MetadataBody body =
                expect(
                        client.call(new Messages.MetadataRequest(topic)),
                        Messages.MetadataBody.class);
        return body.partitions().stream()
                .filter(partition -> partition.tp().equals(tp))
                .findFirst()
                .orElseThrow(
                        () ->
                                new CourseException(
                                        ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                                        "unknown topic partition " + tp));
    }

    private static <T extends Messages.Response> T expect(Messages.Reply reply, Class<T> type) {
        if (reply.error() != ErrorCode.NONE) {
            String message =
                    reply.body() instanceof Messages.ErrorBody error
                            ? error.message()
                            : reply.error().name();
            throw new CourseException(reply.error(), message);
        }
        if (!type.isInstance(reply.body())) throw invalid("unexpected response body for request");
        return type.cast(reply.body());
    }

    private static Endpoint endpoint(String value) {
        int colon = value.lastIndexOf(':');
        if (colon <= 0 || colon == value.length() - 1) throw invalid("bootstrap must be host:port");
        int port = integer(value.substring(colon + 1), "bootstrap port");
        try {
            return new Endpoint(value.substring(0, colon), port);
        } catch (IllegalArgumentException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid bootstrap endpoint", exception);
        }
    }

    private static int integer(String value, String name) {
        try {
            return Integer.parseInt(value);
        } catch (NumberFormatException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid " + name + ": " + value, exception);
        }
    }

    private static long longInteger(String value, String name) {
        try {
            return Long.parseLong(value);
        } catch (NumberFormatException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "invalid " + name + ": " + value, exception);
        }
    }

    private static CourseException invalid(String message) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message);
    }

    private record Arguments(String command, Map<String, String> values) {
        static Arguments parse(String[] args) {
            if (args.length < 3 || !args[0].equals("--bootstrap"))
                throw invalid(
                        "usage: ClientMain --bootstrap host:port produce|fetch --topic NAME --partition N [options]");
            String command = args[2];
            if (!command.equals("produce") && !command.equals("fetch"))
                throw invalid("command must be produce or fetch");
            Map<String, String> values = new HashMap<>();
            values.put("--bootstrap", args[1]);
            for (int index = 3; index < args.length; index++) {
                String option = args[index];
                if (!option.startsWith("--") || index + 1 == args.length)
                    throw invalid("expected --option value, got: " + option);
                if (values.putIfAbsent(option, args[++index]) != null)
                    throw invalid("duplicate option: " + option);
            }
            for (String option : values.keySet()) {
                if (!option.equals("--bootstrap")
                        && !option.equals("--topic")
                        && !option.equals("--partition")
                        && !option.equals("--key")
                        && !option.equals("--value")
                        && !option.equals("--offset")) throw invalid("unknown option: " + option);
            }
            if (command.equals("produce") && !values.containsKey("--value"))
                throw invalid("produce requires --value");
            if (command.equals("fetch")
                    && (values.containsKey("--key") || values.containsKey("--value")))
                throw invalid("fetch does not accept --key or --value");
            return new Arguments(command, Map.copyOf(values));
        }

        String required(String key) {
            String value = values.get(key);
            if (value == null) throw invalid("missing " + key);
            return value;
        }

        String optional(String key) {
            return values.get(key);
        }

        String valueOrDefault(String key, String defaultValue) {
            return values.getOrDefault(key, defaultValue);
        }
    }
}
