package io.simplekafka.course;

import static io.simplekafka.support.TestSupport.assertCode;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.storage.PartitionLog;
import io.simplekafka.support.TempDirectory;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.stream.LongStream;
import java.util.stream.Stream;

class Step07Test {
    @Test
    @DisplayName("Deletes only eligible closed segments and keeps the active segment")
    void deletesOnlyEligibleClosedSegmentsAndKeepsTheActiveSegment() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("orders-0");
            Path notes = directory.resolve("notes.txt");
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                log.append(records(0, 9).stream().map(LogRecord::data).toList());
                assertEquals(1, log.mutationVersion());
                Files.writeString(notes, "retention must not remove unrelated files");
                assertEquals(List.of(0L, 3L, 6L), segmentBases(directory));

                assertEquals(3, log.deleteBefore(4));
                assertEquals(
                        2,
                        log.mutationVersion(),
                        "an actual segment deletion bumps the version once");
                assertEquals(3, log.logStartOffset());
                assertEquals(9, log.logEndOffset());
                assertEquals(List.of(3L, 6L), segmentBases(directory));
                assertFalse(Files.exists(logFile(directory, 0)));
                assertFalse(Files.exists(indexFile(directory, 0)));
                assertTrue(Files.exists(logFile(directory, 3)));
                assertTrue(Files.exists(indexFile(directory, 3)));
                assertTrue(Files.exists(logFile(directory, 6)));
                assertTrue(Files.exists(indexFile(directory, 6)));
                assertTrue(Files.exists(notes));
                assertEquals("retention must not remove unrelated files", Files.readString(notes));
                assertEquals(records(3, 6), log.read(3, 10, 1_024));

                var afterFirstDeletion = directorySnapshot(directory);
                assertEquals(3, log.deleteBefore(2));
                assertEquals(
                        2, log.mutationVersion(), "no-op thresholds do not invalidate a proof");
                assertEquals(afterFirstDeletion, directorySnapshot(directory));
                assertEquals(3, log.deleteBefore(3));
                assertEquals(afterFirstDeletion, directorySnapshot(directory));
                assertEquals(
                        2,
                        log.mutationVersion(),
                        "threshold equal to the retained start is a no-op");

                assertEquals(6, log.deleteBefore(9));
                assertEquals(3, log.mutationVersion());
                assertEquals(6, log.logStartOffset());
                assertEquals(List.of(6L), segmentBases(directory));
                assertFalse(Files.exists(logFile(directory, 3)));
                assertFalse(Files.exists(indexFile(directory, 3)));
                assertTrue(Files.exists(logFile(directory, 6)));
                assertTrue(Files.exists(indexFile(directory, 6)));
                assertEquals(6, log.deleteBefore(1_000));
                assertEquals(
                        3,
                        log.mutationVersion(),
                        "the final active segment is retained without a version bump");
                assertEquals(List.of(6L), segmentBases(directory));

                assertEquals(
                        new AppendResult(9, 10),
                        log.append(List.of(new RecordData(null, new byte[0], 9))));
                assertEquals(records(6, 4), log.read(6, 10, 1_024));
            }

            try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
                assertEquals(6, reopened.logStartOffset());
                assertEquals(10, reopened.logEndOffset());
                assertEquals(records(6, 4), reopened.read(6, 10, 1_024));
                assertCode(ErrorCode.OFFSET_OUT_OF_RANGE, () -> reopened.read(5, 1, 32));
                assertTrue(Files.exists(notes));
                assertEquals(List.of(6L, 9L), segmentBases(directory));
            }
        }
    }

    @Test
    @DisplayName("Retention keeps the only active segment whether empty or nonempty")
    void retentionKeepsTheOnlyActiveSegmentWhetherEmptyOrNonempty() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path emptyDirectory = temp.root().resolve("empty");
            try (PartitionLog empty = new PartitionLog(emptyDirectory, 96, 1)) {
                assertEquals(0, empty.mutationVersion());
                assertEquals(0, empty.deleteBefore(1_000));
                assertCode(ErrorCode.INVALID_REQUEST, () -> empty.deleteBefore(-1));
                assertEquals(
                        0,
                        empty.mutationVersion(),
                        "invalid retention does not change the version");
                assertEquals(0, empty.logStartOffset());
                assertEquals(0, empty.logEndOffset());
                assertEquals(List.of(0L), segmentBases(emptyDirectory));
                assertTrue(Files.exists(logFile(emptyDirectory, 0)));
                assertTrue(Files.exists(indexFile(emptyDirectory, 0)));
            }

            Path oneSegmentDirectory = temp.root().resolve("one-segment");
            try (PartitionLog oneSegment = new PartitionLog(oneSegmentDirectory, 96, 1)) {
                oneSegment.append(records(0, 2).stream().map(LogRecord::data).toList());
                assertEquals(1, oneSegment.mutationVersion());
                var before = directorySnapshot(oneSegmentDirectory);
                assertEquals(0, oneSegment.deleteBefore(1_000));
                assertEquals(
                        1,
                        oneSegment.mutationVersion(),
                        "retaining the only active segment is a no-op");
                assertEquals(0, oneSegment.logStartOffset());
                assertEquals(2, oneSegment.logEndOffset());
                assertEquals(List.of(0L), segmentBases(oneSegmentDirectory));
                assertEquals(before, directorySnapshot(oneSegmentDirectory));
                assertEquals(records(0, 2), oneSegment.read(0, 10, 256));
            }
        }
    }

    @Test
    @DisplayName("Deletes closed segment at its exact end offset")
    void deletesClosedSegmentAtItsExactEndOffset() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path directory = temp.root().resolve("exact-retention-boundary");
            try (PartitionLog log = new PartitionLog(directory, 96, 1)) {
                log.append(records(0, 6).stream().map(LogRecord::data).toList());
                assertEquals(List.of(0L, 3L), segmentBases(directory));

                assertEquals(3, log.deleteBefore(3));
                assertEquals(3, log.logStartOffset());
                assertEquals(6, log.logEndOffset());
                assertEquals(List.of(3L), segmentBases(directory));
                assertFalse(Files.exists(logFile(directory, 0)));
                assertFalse(Files.exists(indexFile(directory, 0)));
                assertTrue(Files.exists(logFile(directory, 3)));
                assertTrue(Files.exists(indexFile(directory, 3)));
                assertEquals(records(3, 3), log.read(3, 3, 96));
            }

            try (PartitionLog reopened = new PartitionLog(directory, 96, 1)) {
                assertEquals(3, reopened.logStartOffset());
                assertEquals(6, reopened.logEndOffset());
                assertEquals(records(3, 3), reopened.read(3, 3, 96));
            }
        }
    }

    private static List<LogRecord> records(long firstOffset, int count) {
        return LongStream.range(firstOffset, firstOffset + count)
                .mapToObj(
                        offset -> new LogRecord(offset, new RecordData(null, new byte[0], offset)))
                .toList();
    }

    private static List<Long> segmentBases(Path directory) throws IOException {
        try (Stream<Path> paths = Files.list(directory)) {
            return paths.filter(Files::isRegularFile)
                    .filter(path -> path.getFileName().toString().matches("[0-9]{20}\\.log"))
                    .sorted()
                    .map(path -> Long.parseLong(path.getFileName().toString().substring(0, 20)))
                    .toList();
        }
    }

    private static Path logFile(Path directory, long base) {
        return directory.resolve(String.format(java.util.Locale.ROOT, "%020d.log", base));
    }

    private static Path indexFile(Path directory, long base) {
        return directory.resolve(String.format(java.util.Locale.ROOT, "%020d.index", base));
    }

    private static java.util.Map<String, String> directorySnapshot(Path directory)
            throws IOException {
        java.util.Map<String, String> snapshot = new java.util.TreeMap<>();
        try (Stream<Path> paths = Files.list(directory)) {
            for (Path path : paths.toList()) {
                String contents =
                        Files.isDirectory(path)
                                ? "<directory>"
                                : java.util.HexFormat.of().formatHex(Files.readAllBytes(path));
                snapshot.put(path.getFileName().toString(), contents);
            }
        }
        return snapshot;
    }
}
