package io.simplekafka.protocol;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.RecordData;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.List;
import java.util.Objects;

/** Incremental content fingerprint for one idempotent producer batch. */
public final class ProducerBatchFingerprint {
    private final MessageDigest digest;
    private final int batchSize;
    private final byte[] scratch = new byte[Long.BYTES];
    private int count;
    private boolean finished;

    public ProducerBatchFingerprint(int batchSize) {
        if (batchSize <= 0)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "batch size must be positive");
        this.batchSize = batchSize;
        try {
            digest = MessageDigest.getInstance("SHA-256");
        } catch (NoSuchAlgorithmException exception) {
            throw new IllegalStateException("SHA-256 is unavailable", exception);
        }
        updateInt(batchSize);
    }

    /** Adds one record's timestamp, key, and value to this batch fingerprint. */
    public void update(RecordData data) {
        if (finished || count >= batchSize)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "batch fingerprint is complete");
        Objects.requireNonNull(data, "data");
        updateLong(data.timestamp());
        byte[] key = data.key();
        updateInt(key == null ? -1 : key.length);
        if (key != null) digest.update(key);
        byte[] value = data.value();
        updateInt(value.length);
        digest.update(value);
        count++;
    }

    /** Finishes this fingerprint after exactly the declared number of records was supplied. */
    public byte[] finish() {
        if (finished || count != batchSize)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "batch fingerprint is incomplete");
        finished = true;
        return digest.digest();
    }

    /** Computes a batch fingerprint and rejects inputs already carrying producer identity. */
    public static byte[] fingerprint(List<RecordData> records) {
        if (records == null || records.isEmpty())
            throw new CourseException(ErrorCode.INVALID_REQUEST, "batch must not be empty");
        ProducerBatchFingerprint fingerprint = new ProducerBatchFingerprint(records.size());
        for (RecordData record : records) {
            if (record == null || record.producerStamp() != null)
                throw new CourseException(
                        ErrorCode.INVALID_REQUEST, "producer input must be an unstamped record");
            fingerprint.update(record);
        }
        return fingerprint.finish();
    }

    private void updateInt(int value) {
        scratch[0] = (byte) (value >>> 24);
        scratch[1] = (byte) (value >>> 16);
        scratch[2] = (byte) (value >>> 8);
        scratch[3] = (byte) value;
        digest.update(scratch, 0, Integer.BYTES);
    }

    private void updateLong(long value) {
        for (int index = Long.BYTES - 1; index >= 0; index--) {
            scratch[index] = (byte) value;
            value >>>= Byte.SIZE;
        }
        digest.update(scratch);
    }
}
