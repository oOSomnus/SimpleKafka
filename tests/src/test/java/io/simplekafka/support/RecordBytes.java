package io.simplekafka.support;

import io.simplekafka.model.LogRecord;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.CRC32C;

/**
 * Independent big-endian record fixtures and disk reader; deliberately does not use RecordCodec.
 */
public final class RecordBytes {
    private static final int MIN_LENGTH = 28;
    private static final int MAX_LENGTH = 1_048_576;
    private static final int FIXED_BYTES = 32;

    private RecordBytes() {}

    /** Encodes ordinary or stamped records independently from the production codec. */
    public static byte[] record(long offset, RecordData data) {
        if (data == null) throw new IllegalArgumentException("record data must not be null");
        if (data.producerStamp() == null)
            return record(offset, data.key(), data.value(), data.timestamp());

        byte[] key = data.key();
        byte[] value = data.value();
        var stamp = data.producerStamp();
        int keyBytes = key == null ? 0 : key.length;
        int totalBytes = Math.addExact(32 + 64, Math.addExact(keyBytes, value.length));
        byte[] bytes = new byte[totalBytes];
        ByteBuffer output = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        output.putInt(bytes.length - Integer.BYTES);
        output.putInt(0);
        output.putLong(offset);
        output.putLong(data.timestamp());
        output.putInt(-2);
        output.putLong(stamp.producerId());
        output.putInt(stamp.producerEpoch());
        output.putLong(stamp.firstSequence());
        output.putInt(stamp.batchSize());
        output.putInt(stamp.batchIndex());
        output.put(stamp.batchHash());
        output.putInt(key == null ? -1 : key.length);
        output.putInt(value.length);
        if (key != null) output.put(key);
        output.put(value);
        repairCrc(bytes);
        return bytes;
    }

    public static byte[] record(long offset, byte[] key, byte[] value, long timestamp) {
        return record(offset, key, value, timestamp, key == null ? -1 : key.length, value.length);
    }

    public static byte[] record(
            long offset,
            byte[] key,
            byte[] value,
            long timestamp,
            int declaredKeyLength,
            int declaredValueLength) {
        if (value == null) throw new IllegalArgumentException("value must not be null");
        int keyBytes = key == null ? 0 : key.length;
        byte[] bytes = new byte[Math.addExact(FIXED_BYTES, Math.addExact(keyBytes, value.length))];
        ByteBuffer output = ByteBuffer.wrap(bytes).order(ByteOrder.BIG_ENDIAN);
        output.putInt(bytes.length - Integer.BYTES);
        output.putInt(0);
        output.putLong(offset);
        output.putLong(timestamp);
        output.putInt(declaredKeyLength);
        output.putInt(declaredValueLength);
        if (key != null) output.put(key);
        output.put(value);
        repairCrc(bytes);
        return bytes;
    }

    public static byte[] withLengthPrefix(byte[] record, int replacementLength) {
        byte[] copy = record.clone();
        ByteBuffer.wrap(copy).order(ByteOrder.BIG_ENDIAN).putInt(0, replacementLength);
        return copy;
    }

    public static byte[] withField(
            byte[] record, int fieldPosition, long value, int width, boolean repairCrc) {
        if (width != 1 && width != 2 && width != 4 && width != 8)
            throw new IllegalArgumentException("field width must be 1, 2, 4, or 8");
        if (fieldPosition < 0 || fieldPosition > record.length - width)
            throw new IllegalArgumentException("field is outside the record");
        byte[] copy = record.clone();
        ByteBuffer output = ByteBuffer.wrap(copy).order(ByteOrder.BIG_ENDIAN);
        switch (width) {
            case 1 -> output.put(fieldPosition, (byte) value);
            case 2 -> output.putShort(fieldPosition, (short) value);
            case 4 -> output.putInt(fieldPosition, (int) value);
            case 8 -> output.putLong(fieldPosition, value);
            default -> throw new AssertionError("unreachable width");
        }
        if (repairCrc) repairCrc(copy);
        return copy;
    }

    public static byte[] concat(byte[]... records) {
        int length = 0;
        for (byte[] record : records) length = Math.addExact(length, record.length);
        byte[] joined = new byte[length];
        int position = 0;
        for (byte[] record : records) {
            System.arraycopy(record, 0, joined, position, record.length);
            position += record.length;
        }
        return joined;
    }

    public static List<LogRecord> readRecords(Path path) {
        byte[] bytes = read(path);
        List<LogRecord> records = new ArrayList<>();
        int position = 0;
        while (position < bytes.length) {
            int totalBytes = checkedTotalLength(bytes, position);
            records.add(parseRecord(bytes, position, totalBytes));
            position += totalBytes;
        }
        return List.copyOf(records);
    }

    public static List<Long> positions(Path path) {
        byte[] bytes = read(path);
        List<Long> positions = new ArrayList<>();
        int position = 0;
        while (position < bytes.length) {
            int totalBytes = checkedTotalLength(bytes, position);
            positions.add((long) position);
            position += totalBytes;
        }
        return List.copyOf(positions);
    }

    private static byte[] read(Path path) {
        try {
            return Files.readAllBytes(path);
        } catch (IOException exception) {
            throw new AssertionError("could not read record fixture " + path, exception);
        }
    }

    private static int checkedTotalLength(byte[] bytes, int position) {
        if (position < 0 || position > bytes.length - Integer.BYTES)
            throw new AssertionError("truncated length prefix at byte " + position);
        int length =
                ByteBuffer.wrap(bytes, position, Integer.BYTES)
                        .order(ByteOrder.BIG_ENDIAN)
                        .getInt();
        if (length < MIN_LENGTH || length > MAX_LENGTH)
            throw new AssertionError("invalid record length " + length + " at byte " + position);
        long total = Integer.BYTES + (long) length;
        if (total > bytes.length - (long) position)
            throw new AssertionError("truncated record body at byte " + position);
        return (int) total;
    }

    private static LogRecord parseRecord(byte[] bytes, int position, int totalBytes) {
        int end = position + totalBytes;
        int expectedCrc =
                ByteBuffer.wrap(bytes, position + Integer.BYTES, Integer.BYTES)
                        .order(ByteOrder.BIG_ENDIAN)
                        .getInt();
        CRC32C crc = new CRC32C();
        crc.update(bytes, position + Integer.BYTES * 2, totalBytes - Integer.BYTES * 2);
        if ((int) crc.getValue() != expectedCrc)
            throw new AssertionError("record CRC mismatch at byte " + position);

        ByteBuffer input =
                ByteBuffer.wrap(
                                bytes,
                                position + Integer.BYTES * 2,
                                end - position - Integer.BYTES * 2)
                        .slice()
                        .order(ByteOrder.BIG_ENDIAN);
        if (input.remaining() < Long.BYTES * 2 + Integer.BYTES)
            throw new AssertionError("record header is incomplete at byte " + position);
        long offset = input.getLong();
        long timestamp = input.getLong();
        int keyOrMarker = input.getInt();
        ProducerStamp stamp = null;
        int keyLength;
        int valueLength;
        if (keyOrMarker == -2) {
            if (input.remaining() < Long.BYTES * 2 + Integer.BYTES * 5 + 32)
                throw new AssertionError("stamped record header is incomplete at byte " + position);
            long producerId = input.getLong();
            int producerEpoch = input.getInt();
            long firstSequence = input.getLong();
            int batchSize = input.getInt();
            int batchIndex = input.getInt();
            byte[] batchHash = new byte[32];
            input.get(batchHash);
            keyLength = input.getInt();
            valueLength = input.getInt();
            try {
                stamp =
                        new ProducerStamp(
                                producerId,
                                producerEpoch,
                                firstSequence,
                                batchSize,
                                batchIndex,
                                batchHash);
            } catch (IllegalArgumentException exception) {
                throw new AssertionError("invalid producer stamp at byte " + position, exception);
            }
        } else {
            keyLength = keyOrMarker;
            if (input.remaining() < Integer.BYTES)
                throw new AssertionError(
                        "record byte-array header is incomplete at byte " + position);
            valueLength = input.getInt();
        }
        if (offset < 0 || timestamp < 0 || keyLength < -1 || valueLength < 0)
            throw new AssertionError("invalid record fields at byte " + position);
        long payloadBytes = (keyLength < 0 ? 0L : keyLength) + (long) valueLength;
        if (payloadBytes != input.remaining())
            throw new AssertionError("record payload lengths do not match at byte " + position);
        byte[] key = null;
        if (keyLength >= 0) {
            key = new byte[keyLength];
            input.get(key);
        }
        byte[] value = new byte[valueLength];
        input.get(value);
        return new LogRecord(offset, new RecordData(key, value, timestamp, stamp));
    }

    private static void repairCrc(byte[] bytes) {
        CRC32C crc = new CRC32C();
        crc.update(bytes, Integer.BYTES * 2, bytes.length - Integer.BYTES * 2);
        ByteBuffer.wrap(bytes)
                .order(ByteOrder.BIG_ENDIAN)
                .putInt(Integer.BYTES, (int) crc.getValue());
    }
}
