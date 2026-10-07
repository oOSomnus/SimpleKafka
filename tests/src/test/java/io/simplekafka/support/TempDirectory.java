package io.simplekafka.support;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Comparator;
import java.util.stream.Stream;

/** A real temporary directory removed recursively when a test finishes. */
public final class TempDirectory implements AutoCloseable {
    private final Path root;

    public TempDirectory() throws IOException {
        root = Files.createTempDirectory("simple-kafka-test-");
    }

    public Path root() {
        return root;
    }

    @Override
    public void close() throws IOException {
        if (!Files.exists(root)) return;
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.sorted(Comparator.reverseOrder()).toList()) {
                Files.deleteIfExists(path);
            }
        }
    }
}
