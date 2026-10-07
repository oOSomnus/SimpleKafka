package io.simplekafka.protocol;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Big-endian framing for the course protocol; this is not Kafka's wire protocol. */
public final class FrameCodec {
    private static final short VERSION = 1;
    private static final int MIN_FRAME_LENGTH = 10;
    private static final int MAX_FRAME_LENGTH = 8_388_608;

    private FrameCodec() {}

    public static void write(OutputStream output, Frame frame) throws IOException {
        if (output == null || frame == null) throw invalid("output and frame are required");
        Api.requireKnown(frame.api());
        byte[] payload = frame.payload();
        long frameLength = (long) MIN_FRAME_LENGTH + payload.length;
        if (frameLength > MAX_FRAME_LENGTH) throw invalid("frame exceeds maximum length");

        writeInt(output, (int) frameLength);
        writeShort(output, VERSION);
        writeShort(output, frame.api());
        writeInt(output, frame.correlationId());
        writeShort(output, frame.error().wireId());
        output.write(payload);
    }

    public static Frame read(InputStream input) throws IOException {
        if (input == null) throw invalid("input is required");
        int first = input.read();
        if (first < 0) return null;
        int length = (first << 24) | (readRequired(input) << 16) | (readRequired(input) << 8) | readRequired(input);
        if (length < MIN_FRAME_LENGTH || length > MAX_FRAME_LENGTH)
            throw invalid("invalid frame length: " + Integer.toUnsignedString(length));

        short version = readShort(input);
        if (version != VERSION) throw invalid("unsupported protocol version: " + version);
        short api = readShort(input);
        Api.requireKnown(api);
        int correlationId = readInt(input);
        ErrorCode error = ErrorCode.fromWireId(readShort(input));

        int payloadLength = length - MIN_FRAME_LENGTH;
        byte[] payload = new byte[payloadLength];
        readFully(input, payload, 0, payloadLength);
        return new Frame(api, correlationId, error, payload);
    }

    private static void writeShort(OutputStream output, short value) throws IOException {
        output.write((value >>> 8) & 0xff);
        output.write(value & 0xff);
    }

    private static void writeInt(OutputStream output, int value) throws IOException {
        output.write((value >>> 24) & 0xff);
        output.write((value >>> 16) & 0xff);
        output.write((value >>> 8) & 0xff);
        output.write(value & 0xff);
    }

    private static short readShort(InputStream input) throws IOException {
        return (short) ((readRequired(input) << 8) | readRequired(input));
    }

    private static int readInt(InputStream input) throws IOException {
        return (readRequired(input) << 24) | (readRequired(input) << 16)
                | (readRequired(input) << 8) | readRequired(input);
    }

    private static int readRequired(InputStream input) throws IOException {
        int value = input.read();
        if (value < 0) throw invalid("truncated frame");
        return value;
    }

    private static void readFully(InputStream input, byte[] destination, int offset, int length) throws IOException {
        int end = offset + length;
        while (offset < end) {
            int count = input.read(destination, offset, end - offset);
            if (count < 0) throw invalid("truncated frame payload");
            if (count == 0) {
                int value = input.read();
                if (value < 0) throw invalid("truncated frame payload");
                destination[offset++] = (byte) value;
            } else {
                offset += count;
            }
        }
    }

    private static CourseException invalid(String message) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message);
    }
}
