package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.client.Partitioner;
import java.nio.charset.StandardCharsets;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertEquals;

class Step09Test {
    @Test
    void routesKeysByTheUnsignedCrc32cValueAndKeepsEmptyKeyAsAKey() {
        Partitioner partitioner = new Partitioner();
        byte[] crc32cCheckVector = "123456789".getBytes(StandardCharsets.US_ASCII);
        assertEquals(2, partitioner.choose(crc32cCheckVector, 7));
        assertEquals(2, partitioner.choose(crc32cCheckVector, 7));
        assertEquals(0, partitioner.choose(new byte[0], 3));
    }

    @Test
    void roundRobinsNullKeysPerInstanceAndRequiresAPositivePartitionCount() {
        Partitioner first = new Partitioner();
        assertEquals(java.util.List.of(0, 1, 2, 0, 1, 2),
                java.util.stream.IntStream.range(0, 6).map(ignored -> first.choose(null, 3)).boxed().toList());
        assertEquals(0, new Partitioner().choose(null, 3), "round-robin state is instance-local");
        assertCode(ErrorCode.INVALID_REQUEST, () -> first.choose(null, 0));
        assertCode(ErrorCode.INVALID_REQUEST, () -> first.choose(new byte[]{1}, -2));
    }
}
