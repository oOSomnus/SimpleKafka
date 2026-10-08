package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.Partitioner;
import java.nio.charset.StandardCharsets;
import java.util.zip.CRC32C;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step09Test {
    @Test
    @DisplayName("Routes by known unsigned CRC32C vectors and keeps empty key as a key")
    void routesByKnownUnsignedCrc32cVectorsAndKeepsEmptyKeyAsAKey() {
        byte[] checkVector = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0xe3069283L, crc32c(checkVector), "CRC32C standard check vector");

        byte[] key = "a".getBytes(StandardCharsets.US_ASCII);
        assertEquals(0xc1d04330L, crc32c(key), "CRC32C for the key 'a'");

        Partitioner partitioner = new Partitioner();
        assertEquals(2, partitioner.choose(checkVector, 7));
        assertEquals(1, partitioner.choose(key, 7));

        byte[] emptyKey = new byte[0];
        assertEquals(0L, crc32c(emptyKey), "CRC32C of an empty key");
        assertEquals(0, partitioner.choose(emptyKey, 7));
    }

    @Test
    @DisplayName("Keyed and empty keys do not advance the null key cursor")
    void keyedAndEmptyKeysDoNotAdvanceTheNullKeyCursor() {
        Partitioner partitioner = new Partitioner();

        assertEquals(0, partitioner.choose(null, 3));
        assertEquals(1, partitioner.choose("a".getBytes(StandardCharsets.US_ASCII), 7));
        assertEquals(0, partitioner.choose(new byte[0], 7));
        assertEquals(1, partitioner.choose(null, 3));
    }

    @Test
    @DisplayName("Null key cursors are independent and single partition always selects zero")
    void nullKeyCursorsAreIndependentAndSinglePartitionAlwaysSelectsZero() {
        Partitioner first = new Partitioner();
        Partitioner second = new Partitioner();
        assertEquals(0, first.choose(null, 3));
        assertEquals(0, second.choose(null, 3), "round-robin state is instance-local");
        assertEquals(1, first.choose(null, 3));
        assertEquals(1, second.choose(null, 3));

        Partitioner singlePartition = new Partitioner();
        assertEquals(0, singlePartition.choose(null, 1));
        assertEquals(0, singlePartition.choose("a".getBytes(StandardCharsets.US_ASCII), 1));
        assertEquals(0, singlePartition.choose(new byte[0], 1));
        assertEquals(0, singlePartition.choose(null, 1));
    }

    @Test
    @DisplayName("Null key cursor stays within partitions when topic sizes change")
    void nullKeyCursorStaysWithinPartitionsWhenTopicSizesChange() {
        Partitioner partitioner = new Partitioner();

        assertEquals(0, partitioner.choose(null, 3));
        assertEquals(1, partitioner.choose(null, 3));
        assertEquals(0, partitioner.choose(null, 1));
        assertEquals(0, partitioner.choose(null, 3));
        assertEquals(1, partitioner.choose(null, 3));
        assertEquals(0, partitioner.choose(null, 2));
        assertEquals(1, partitioner.choose(null, 2));
    }

    @Test
    @DisplayName("Requires a positive partition count")
    void requiresAPositivePartitionCount() {
        Partitioner partitioner = new Partitioner();
        assertCode(ErrorCode.INVALID_REQUEST, () -> partitioner.choose(null, 0));
        assertCode(ErrorCode.INVALID_REQUEST,
                () -> partitioner.choose(new byte[]{1}, -1));
        assertEquals(0, partitioner.choose(null, 3));
        assertEquals(1, partitioner.choose(null, 3));
    }

    private static long crc32c(byte[] bytes) {
        CRC32C checksum = new CRC32C();
        checksum.update(bytes, 0, bytes.length);
        return checksum.getValue();
    }
}
