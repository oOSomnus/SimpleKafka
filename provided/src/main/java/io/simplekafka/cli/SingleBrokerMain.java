package io.simplekafka.cli;

import io.simplekafka.model.Endpoint;
import io.simplekafka.support.BrokerHarness;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.CountDownLatch;

/** Runs a real loopback TCP broker until the process receives a shutdown signal. */
public final class SingleBrokerMain {
    private SingleBrokerMain() {}

    public static void main(String[] args) throws InterruptedException {
        for (String arg : args) {
            if (arg.equals("--help")) {
                System.out.println("Usage: SingleBrokerMain [--data-dir PATH] [--broker-id N] [--port N] [--topic NAME] [--partitions N]");
                return;
            }
        }
        Options options = Options.parse(args);
        try (BrokerHarness harness = BrokerHarness.single(options.dataDirectory(), options.brokerId(),
                options.port(), options.topic(), options.partitions())) {
            Endpoint endpoint = harness.endpoint();
            CountDownLatch stopped = new CountDownLatch(1);
            Thread hook = new Thread(() -> {
                try {
                    harness.close();
                } finally {
                    stopped.countDown();
                }
            }, "simple-kafka-shutdown");
            Runtime.getRuntime().addShutdownHook(hook);
            System.out.printf("LISTEN host=%s port=%d%n", endpoint.host(), endpoint.port());
            System.out.flush();
            try {
                stopped.await();
            } finally {
                harness.close();
            }
        }
    }

    private record Options(Path dataDirectory, int brokerId, int port, String topic, int partitions) {
        private static Options parse(String[] args) {
            Map<String, String> values = new HashMap<>();
            for (int i = 0; i < args.length; i++) {
                String name = args[i];
                if (!name.startsWith("--") || i + 1 == args.length)
                    throw new IllegalArgumentException("expected --option value, got: " + name);
                if (values.putIfAbsent(name, args[++i]) != null)
                    throw new IllegalArgumentException("duplicate option: " + name);
            }
            Path directory = Path.of(values.getOrDefault("--data-dir", "build/single-broker"));
            int brokerId = integer(values.getOrDefault("--broker-id", "1"), "broker id");
            int port = integer(values.getOrDefault("--port", "0"), "port");
            String topic = values.getOrDefault("--topic", "demo");
            int partitions = integer(values.getOrDefault("--partitions", "1"), "partitions");
            for (String option : values.keySet()) {
                if (!option.equals("--data-dir") && !option.equals("--broker-id") && !option.equals("--port")
                        && !option.equals("--topic") && !option.equals("--partitions"))
                    throw new IllegalArgumentException("unknown option: " + option);
            }
            if (brokerId < 0 || port < 0 || port > 65535 || partitions <= 0)
                throw new IllegalArgumentException("broker id, port, or partition count is out of range");
            return new Options(directory, brokerId, port, topic, partitions);
        }

        private static int integer(String value, String name) {
            try { return Integer.parseInt(value); }
            catch (NumberFormatException exception) { throw new IllegalArgumentException("invalid " + name + ": " + value, exception); }
        }
    }
}
