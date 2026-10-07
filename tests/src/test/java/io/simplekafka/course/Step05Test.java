package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.model.IndexEntry;
import io.simplekafka.model.LogRecord;
import io.simplekafka.storage.RecordCodec;
import io.simplekafka.storage.SegmentLog;
import io.simplekafka.storage.SparseIndex;
import io.simplekafka.support.TempDirectory;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static io.simplekafka.support.TestSupport.value;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step05Test {
    @Test
    void storesPeriodicEntriesAndReturnsTheGreatestEntryNotAfterTheTarget() throws Exception {
        try (TempDirectory temp = new TempDirectory();
             SparseIndex index = new SparseIndex(temp.root().resolve("segment.index"), 0, 4)) {
            index.add(0, 0);
            index.add(1, 32);
            index.add(4, 128);
            index.add(8, 256);

            assertEquals(new IndexEntry(0, 0), index.floor(3));
            assertEquals(new IndexEntry(4, 128), index.floor(6));
            assertEquals(new IndexEntry(8, 256), index.floor(20));
            assertCode(ErrorCode.INVALID_REQUEST, () -> index.add(7, 300));
        }
    }

    @Test
    void rebuildsFromTheLogWhenTheIndexIsMissingOrMalformed() throws Exception {
        try (TempDirectory temp = new TempDirectory()) {
            Path logFile = temp.root().resolve("segment.log");
            Path indexFile = temp.root().resolve("segment.index");
            try (SegmentLog log = SegmentLog.create(logFile, 0)) {
                log.append(java.util.stream.IntStream.range(0, 9).mapToObj(i -> value("r" + i)).toList());
            }

            try (SparseIndex missing = new SparseIndex(indexFile, 0, 4)) {
                missing.rebuild(logFile, 0);
                assertEquals(new IndexEntry(4, positionAt(logFile, 4)), missing.floor(6));
            }

            Files.write(indexFile, new byte[]{1, 2, 3});
            try (SparseIndex wrongLength = new SparseIndex(indexFile, 0, 4)) {
                wrongLength.rebuild(logFile, 0);
                assertEquals(new IndexEntry(4, positionAt(logFile, 4)), wrongLength.floor(6));
            }

            ByteBuffer invalidEntry = ByteBuffer.allocate(Long.BYTES * 2).order(ByteOrder.BIG_ENDIAN)
                    .putLong(-1).putLong(-1);
            Files.write(indexFile, invalidEntry.array());
            try (SparseIndex invalidValues = new SparseIndex(indexFile, 0, 4)) {
                invalidValues.rebuild(logFile, 0);
                assertEquals(new IndexEntry(8, positionAt(logFile, 8)), invalidValues.floor(8));
            }
        }
    }

    private static long positionAt(Path logFile, int offset) throws Exception {
        ByteBuffer bytes = ByteBuffer.wrap(Files.readAllBytes(logFile));
        long position = 0;
        for (int current = 0; current < offset; current++) {
            bytes.position(Math.toIntExact(position));
            LogRecord record = RecordCodec.decode(bytes);
            position += RecordCodec.encode(record).remaining();
        }
        return position;
    }
}
