package io.simplekafka.course;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.broker.PartitionCatalog;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.PartitionMetadata;
import io.simplekafka.model.RecordData;
import io.simplekafka.model.TopicPartition;
import io.simplekafka.support.BrokerHarness;
import io.simplekafka.support.TempDirectory;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HexFormat;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.Stream;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

class Step10Test {
    @Test
    void createsIndependentFixedTopicsAndReopensOnlyWithExplicitPartitionCounts() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            List<PartitionMetadata> ordersBeforeRestart;
            List<PartitionMetadata> paymentsBeforeRestart;
            try (BrokerHarness broker = BrokerHarness.single(temp.root(), 7, 0)) {
                PartitionCatalog catalog = broker.catalog();
                catalog.createTopic("orders", 2);
                TopicPartition orders0 = new TopicPartition("orders", 0);
                RecordData firstValue = new RecordData(null, "v0".getBytes(StandardCharsets.UTF_8), 0);
                RecordData secondValue = new RecordData(null, "v1".getBytes(StandardCharsets.UTF_8), 1);
                List<LogRecord> expectedOrders = List.of(new LogRecord(0, firstValue), new LogRecord(1, secondValue));
                assertEquals(new AppendResult(0, 2),
                        catalog.partition(orders0).append(List.of(firstValue, secondValue)));
                assertEquals(expectedOrders, catalog.partition(orders0).read(0, 10, 1_024));
                List<PartitionMetadata> firstOrdersMetadata = catalog.metadata("orders");
                Map<String, String> ordersBeforeRepeat = treeSnapshot(temp.root().resolve("orders"));

                catalog.createTopic("orders", 2);
                assertEquals(firstOrdersMetadata, catalog.metadata("orders"));
                assertEquals(ordersBeforeRepeat, treeSnapshot(temp.root().resolve("orders")),
                        "repeating the same partition count is idempotent on disk");
                assertEquals(expectedOrders, catalog.partition(orders0).read(0, 10, 1_024));

                Path strayPartition = temp.root().resolve("orders").resolve("9");
                Files.createDirectories(strayPartition);
                Files.writeString(strayPartition.resolve("stray.txt"), "keep");
                List<PartitionMetadata> ordersWithStray = catalog.metadata("orders");
                assertSingleNodeMetadata(broker, "orders", 2, ordersWithStray);
                assertEquals(List.of(0, 1),
                        ordersWithStray.stream().map(item -> item.tp().partition()).toList(),
                        "metadata ignores an unregistered numeric directory");
                assertTrue(Files.isDirectory(strayPartition));

                Map<String, String> ordersBeforeDifferentCount = treeSnapshot(temp.root().resolve("orders"));
                assertCode(ErrorCode.INVALID_REQUEST, () -> catalog.createTopic("orders", 3));
                assertEquals(ordersWithStray, catalog.metadata("orders"),
                        "a conflicting count leaves original metadata unchanged");
                assertEquals(ordersBeforeDifferentCount, treeSnapshot(temp.root().resolve("orders")),
                        "a conflicting count leaves the original topic directory unchanged");
                assertEquals(expectedOrders, catalog.partition(orders0).read(0, 10, 1_024));

                catalog.createTopic("payments", 3);
                List<PartitionMetadata> payments = catalog.metadata("payments");
                assertSingleNodeMetadata(broker, "payments", 3, payments);
                assertSingleNodeMetadata(broker, "orders", 2, catalog.metadata("orders"));
                assertEquals("keep", Files.readString(strayPartition.resolve("stray.txt")));

                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, () -> catalog.metadata("missing"));
                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION,
                        () -> catalog.partition(new TopicPartition("orders", 9)));

                ordersBeforeRestart = catalog.metadata("orders");
                paymentsBeforeRestart = payments;
            }

            try (BrokerHarness restarted = BrokerHarness.single(temp.root(), 7, 0)) {
                PartitionCatalog catalog = restarted.catalog();
                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, () -> catalog.metadata("orders"));
                assertCode(ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, () -> catalog.metadata("payments"));

                catalog.createTopic("orders", 2);
                TopicPartition orders0 = new TopicPartition("orders", 0);
                RecordData firstValue = new RecordData(null, "v0".getBytes(StandardCharsets.UTF_8), 0);
                RecordData secondValue = new RecordData(null, "v1".getBytes(StandardCharsets.UTF_8), 1);
                RecordData thirdValue = new RecordData(null, "v2".getBytes(StandardCharsets.UTF_8), 2);
                List<LogRecord> expectedOrders = List.of(
                        new LogRecord(0, firstValue), new LogRecord(1, secondValue), new LogRecord(2, thirdValue));
                assertEquals(expectedOrders.subList(0, 2),
                        catalog.partition(orders0).read(0, 10, 1_024));
                assertCode(ErrorCode.INVALID_REQUEST, () -> catalog.createTopic("orders", 3));
                assertEquals(new AppendResult(2, 3), catalog.partition(orders0).append(List.of(thirdValue)));
                assertEquals(expectedOrders, catalog.partition(orders0).read(0, 10, 1_024));
                catalog.createTopic("payments", 3);
                List<PartitionMetadata> ordersAfterRestart = catalog.metadata("orders");
                List<PartitionMetadata> paymentsAfterRestart = catalog.metadata("payments");
                assertSingleNodeMetadata(restarted, "orders", 2, ordersAfterRestart);
                assertSingleNodeMetadata(restarted, "payments", 3, paymentsAfterRestart);
                assertEquals(ordersBeforeRestart.stream().map(PartitionMetadata::tp).toList(),
                        ordersAfterRestart.stream().map(PartitionMetadata::tp).toList());
                assertEquals(paymentsBeforeRestart.stream().map(PartitionMetadata::tp).toList(),
                        paymentsAfterRestart.stream().map(PartitionMetadata::tp).toList());
            }
        }
    }

    @Test
    void rejectsInvalidCountsAndTopicNamesWithoutMutatingTheFilesystemOrEscapingTheRoot() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path sandbox = temp.root();
            Path root = sandbox.resolve("catalog");
            Path escapedPath = sandbox.resolve("escaped");
            assertFalse(Files.exists(escapedPath), "the sandbox path-escape target must start absent");
            try (BrokerHarness broker = BrokerHarness.single(root, 7, 0)) {
                PartitionCatalog catalog = broker.catalog();
                Map<String, String> initialTree = treeSnapshot(sandbox);

                for (int partitionCount : List.of(0, -1)) {
                    assertCode(ErrorCode.INVALID_REQUEST,
                            () -> catalog.createTopic("invalid-count", partitionCount));
                    assertEquals(initialTree, treeSnapshot(sandbox),
                            "invalid partition count must not mutate the catalog directory");
                }

                List<String> invalidTopics = List.of(
                        "", ".", "..", "bad/name", " ", "white space", "café", "a".repeat(256));
                for (String topic : invalidTopics) {
                    assertCode(ErrorCode.INVALID_REQUEST, () -> catalog.createTopic(topic, 1));
                    assertEquals(initialTree, treeSnapshot(sandbox),
                            "invalid topic must not mutate the catalog directory: [" + topic + "]");
                }

                String pathEscapingTopic = "../" + escapedPath.getFileName();
                assertCode(ErrorCode.INVALID_REQUEST, () -> catalog.createTopic(pathEscapingTopic, 1));
                assertEquals(initialTree, treeSnapshot(sandbox),
                        "a path-escaping topic must not mutate the sandbox");
                assertFalse(Files.exists(escapedPath), "a path-escaping topic must not create a sibling path");
            }
        }
    }

    @Test
    void acceptsA255ByteTopicIdentifierWhereTheFilesystemAllowsIt() throws Exception {
        String topic = "t".repeat(255);
        assertEquals(255, topic.getBytes(StandardCharsets.UTF_8).length);
        TopicPartition acceptedIdentifier = assertDoesNotThrow(
                () -> new TopicPartition(topic, 0),
                "TopicPartition accepts a 255-byte identifier even if a filesystem component limit prevents creation");

        try (TempDirectory temp = new TempDirectory();
             BrokerHarness broker = BrokerHarness.single(temp.root(), 7, 0)) {
            try {
                broker.catalog().createTopic(topic, 1);
                assertEquals(acceptedIdentifier, broker.catalog().metadata(topic).get(0).tp());
            } catch (CourseException failure) {
                assertTrue(isFileNameTooLong(failure),
                        "If the filesystem component-name limit prevents creating a 255-byte topic, only "
                                + "TopicPartition acceptance is required; unexpected failure: " + failure);
            }
        }
    }

    private static void assertSingleNodeMetadata(BrokerHarness broker, String topic, int partitionCount,
                                                 List<PartitionMetadata> metadata) {
        assertEquals(partitionCount, metadata.size());
        for (int partition = 0; partition < partitionCount; partition++) {
            PartitionMetadata item = metadata.get(partition);
            assertEquals(new TopicPartition(topic, partition), item.tp());
            assertEquals(broker.brokerId(), item.leaderId());
            assertEquals(0, item.epoch());
            assertEquals(List.of(broker.brokerId()), item.replicas());
            assertEquals(List.of(broker.brokerId()), item.isr());
            assertEquals(broker.endpoint(), item.leader());
        }
    }

    private static Map<String, String> treeSnapshot(Path root) throws IOException {
        Map<String, String> snapshot = new TreeMap<>();
        try (Stream<Path> paths = Files.walk(root)) {
            for (Path path : paths.toList()) {
                String relative = root.relativize(path).toString();
                String contents = Files.isDirectory(path)
                        ? "<directory>"
                        : HexFormat.of().formatHex(Files.readAllBytes(path));
                snapshot.put(relative, contents);
            }
        }
        return snapshot;
    }

    private static boolean isFileNameTooLong(Throwable failure) {
        for (Throwable cause = failure; cause != null; cause = cause.getCause()) {
            if (cause instanceof FileSystemException fileSystemFailure) {
                String reason = fileSystemFailure.getReason();
                if (reason != null && reason.toLowerCase(Locale.ROOT).contains("file name too long")) return true;
            }
        }
        return false;
    }

}
