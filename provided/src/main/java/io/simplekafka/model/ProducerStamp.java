package io.simplekafka.model;

import java.util.Arrays;
import java.util.Objects;

/** Durable identity and content fingerprint for one record in an idempotent producer batch. */
public record ProducerStamp(
        long producerId,
        int producerEpoch,
        long firstSequence,
        int batchSize,
        int batchIndex,
        byte[] batchHash) {
    /** Bytes added by the stamp marker and fields to an ordinary record encoding. */
    public static final int ENCODED_OVERHEAD = 64;

    public ProducerStamp {
        Objects.requireNonNull(batchHash, "batchHash");
        if (producerId < 0
                || producerEpoch < 0
                || firstSequence < 0
                || batchSize <= 0
                || batchIndex < 0
                || batchIndex >= batchSize
                || batchHash.length != 32) {
            throw new IllegalArgumentException("producer stamp fields are invalid");
        }
        try {
            Math.addExact(firstSequence, batchSize);
        } catch (ArithmeticException exception) {
            throw new IllegalArgumentException("producer sequence range overflows", exception);
        }
    }

    @Override
    public boolean equals(Object other) {
        return this == other
                || other instanceof ProducerStamp stamp
                        && producerId == stamp.producerId
                        && producerEpoch == stamp.producerEpoch
                        && firstSequence == stamp.firstSequence
                        && batchSize == stamp.batchSize
                        && batchIndex == stamp.batchIndex
                        && Arrays.equals(batchHash, stamp.batchHash);
    }

    @Override
    public int hashCode() {
        int result = Long.hashCode(producerId);
        result = 31 * result + Integer.hashCode(producerEpoch);
        result = 31 * result + Long.hashCode(firstSequence);
        result = 31 * result + Integer.hashCode(batchSize);
        result = 31 * result + Integer.hashCode(batchIndex);
        return 31 * result + Arrays.hashCode(batchHash);
    }
}
