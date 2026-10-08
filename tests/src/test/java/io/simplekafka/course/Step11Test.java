package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import java.util.HexFormat;
import java.util.List;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.DisplayName;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;

class Step11Test {
    private static final byte[] GOLDEN = hex("0000000d00010001000000290008010203");

    @Test
    @DisplayName("Writes and reads the independent golden frame and handles empty payload")
    void writesAndReadsTheIndependentGoldenFrameAndHandlesEmptyPayload() throws Exception {
        Frame expected = new Frame(Api.METADATA, 41, ErrorCode.REQUEST_TIMEOUT, new byte[]{1, 2, 3});
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.write(output, expected);
        assertArrayEquals(GOLDEN, output.toByteArray());
        assertFrameEquals(expected, FrameCodec.read(new ByteArrayInputStream(GOLDEN)));

        Frame empty = new Frame(Api.METADATA, 1, ErrorCode.NONE, new byte[0]);
        ByteArrayOutputStream emptyOutput = new ByteArrayOutputStream();
        FrameCodec.write(emptyOutput, empty);
        assertArrayEquals(hex("0000000a00010001000000010000"), emptyOutput.toByteArray());
        assertFrameEquals(empty, FrameCodec.read(new ByteArrayInputStream(emptyOutput.toByteArray())));
    }
    @Test
    @DisplayName("Preserves independent wire ids for every API and metadata error")
    void preservesIndependentWireIdsForEveryApiAndMetadataError() throws Exception {
        List<HeaderWireCase> cases = List.of(
                new HeaderWireCase((short) 2, ErrorCode.NONE, "0000000a00010002000000290000"),
                new HeaderWireCase((short) 3, ErrorCode.NONE, "0000000a00010003000000290000"),
                new HeaderWireCase((short) 4, ErrorCode.NONE, "0000000a00010004000000290000"),
                new HeaderWireCase((short) 5, ErrorCode.NONE, "0000000a00010005000000290000"),
                new HeaderWireCase((short) 6, ErrorCode.NONE, "0000000a00010006000000290000"),
                new HeaderWireCase((short) 7, ErrorCode.NONE, "0000000a00010007000000290000"),
                new HeaderWireCase((short) 8, ErrorCode.NONE, "0000000a00010008000000290000"),
                new HeaderWireCase((short) 9, ErrorCode.NONE, "0000000a00010009000000290000"),
                new HeaderWireCase((short) 10, ErrorCode.NONE, "0000000a0001000a000000290000"),
                new HeaderWireCase((short) 1, ErrorCode.INVALID_REQUEST, "0000000a00010001000000290001"),
                new HeaderWireCase((short) 1, ErrorCode.UNKNOWN_TOPIC_OR_PARTITION, "0000000a00010001000000290002"),
                new HeaderWireCase((short) 1, ErrorCode.OFFSET_OUT_OF_RANGE, "0000000a00010001000000290003"),
                new HeaderWireCase((short) 1, ErrorCode.CORRUPT_RECORD, "0000000a00010001000000290004"),
                new HeaderWireCase((short) 1, ErrorCode.NOT_LEADER, "0000000a00010001000000290005"),
                new HeaderWireCase((short) 1, ErrorCode.FENCED_EPOCH, "0000000a00010001000000290006"),
                new HeaderWireCase((short) 1, ErrorCode.NOT_ENOUGH_REPLICAS, "0000000a00010001000000290007"),
                new HeaderWireCase((short) 1, ErrorCode.ILLEGAL_GENERATION, "0000000a00010001000000290009"),
                new HeaderWireCase((short) 1, ErrorCode.UNKNOWN_MEMBER, "0000000a0001000100000029000a"),
                new HeaderWireCase((short) 1, ErrorCode.NOT_ASSIGNED, "0000000a0001000100000029000b"),
                new HeaderWireCase((short) 1, ErrorCode.NO_ELIGIBLE_LEADER, "0000000a0001000100000029000c"),
                new HeaderWireCase((short) 1, ErrorCode.STORAGE_ERROR, "0000000a0001000100000029000d"));

        for (HeaderWireCase wireCase : cases) {
            Frame expected = new Frame(wireCase.api(), 41, wireCase.error(), new byte[0]);
            byte[] golden = hex(wireCase.goldenHex());
            assertArrayEquals(golden, encode(expected), wireCase.toString());
            assertFrameEquals(expected, FrameCodec.read(new ByteArrayInputStream(golden)));
        }
    }


    @Test
    @DisplayName("Accepts maximum frame and rejects one byte over before writing anything")
    void acceptsMaximumFrameAndRejectsOneByteOverBeforeWritingAnything() throws Exception {
        byte[] maximumPayload = new byte[8_388_598];
        Arrays.fill(maximumPayload, (byte) 0x5a);
        Frame maximum = new Frame(Api.FETCH, 17, ErrorCode.NONE, maximumPayload);
        ByteArrayOutputStream maximumOutput = new ByteArrayOutputStream(8_388_608);
        FrameCodec.write(maximumOutput, maximum);
        byte[] encoded = maximumOutput.toByteArray();
        assertEquals(8_388_612, encoded.length, "wire size includes the four-byte length prefix");
        assertArrayEquals(hex("00800000"), Arrays.copyOf(encoded, Integer.BYTES));
        assertFrameEquals(maximum, FrameCodec.read(new ByteArrayInputStream(encoded)));

        byte[] oversizedPayload = Arrays.copyOf(maximumPayload, maximumPayload.length + 1);
        ByteArrayOutputStream rejectedOutput = new ByteArrayOutputStream();
        assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.write(rejectedOutput,
                new Frame(Api.FETCH, 18, ErrorCode.NONE, oversizedPayload)));
        assertEquals(0, rejectedOutput.size(), "an oversized frame must not write a partial header");
    }

    @Test
    @DisplayName("Rejects every incomplete prefix of the golden frame")
    void rejectsEveryIncompletePrefixOfTheGoldenFrame() throws Exception {
        for (int length = 0; length < GOLDEN.length; length++) {
            byte[] prefix = Arrays.copyOf(GOLDEN, length);
            if (length == 0) {
                assertNull(FrameCodec.read(new ByteArrayInputStream(prefix)), "prefix length " + length);
            } else {
                assertCode(ErrorCode.INVALID_REQUEST,
                        () -> FrameCodec.read(new ByteArrayInputStream(prefix)));
            }
        }
    }

    @Test
    @DisplayName("Reads back to back frames from streams delivering one two or three bytes at a time")
    void readsBackToBackFramesFromStreamsDeliveringOneTwoOrThreeBytesAtATime() throws Exception {
        Frame first = new Frame(Api.METADATA, 41, ErrorCode.REQUEST_TIMEOUT, new byte[]{1, 2, 3});
        Frame second = new Frame(Api.FETCH, 42, ErrorCode.NONE, new byte[]{9, 8, 7, 6, 5});
        byte[] wire = concat(GOLDEN, encode(second));

        for (int fragmentSize = 1; fragmentSize <= 3; fragmentSize++) {
            InputStream fragmented = fragmented(wire, fragmentSize);
            assertFrameEquals(first, FrameCodec.read(fragmented));
            assertFrameEquals(second, FrameCodec.read(fragmented));
            assertNull(FrameCodec.read(fragmented));
        }
    }

    @Test
    @DisplayName("Rejects invalid lengths without consuming bytes after the header")
    void rejectsInvalidLengthsWithoutConsumingBytesAfterTheHeader() {
        for (int length : new int[]{-1, 0, 9, 8_388_609, Integer.MIN_VALUE}) {
            byte[] bytes = ByteBuffer.allocate(Integer.BYTES + 1).order(ByteOrder.BIG_ENDIAN)
                    .putInt(length).put((byte) 99).array();
            ByteArrayInputStream input = new ByteArrayInputStream(bytes);
            assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(input));
            assertEquals(1, input.available(), "length " + length + " consumed the sentinel byte");
        }
    }

    @Test
    @DisplayName("Rejects unknown version API and error ids")
    void rejectsUnknownVersionApiAndErrorIds() {
        byte[] badVersion = GOLDEN.clone();
        ByteBuffer.wrap(badVersion).order(ByteOrder.BIG_ENDIAN).putShort(4, (short) 2);
        assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(new ByteArrayInputStream(badVersion)));

        for (short api : new short[]{0, 11}) {
            byte[] badApi = GOLDEN.clone();
            ByteBuffer.wrap(badApi).order(ByteOrder.BIG_ENDIAN).putShort(6, api);
            assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(new ByteArrayInputStream(badApi)));
        }
        assertCode(ErrorCode.INVALID_REQUEST,
                () -> FrameCodec.write(new ByteArrayOutputStream(),
                        new Frame((short) 11, 1, ErrorCode.NONE, new byte[0])));

        for (short errorId : new short[]{99, -1}) {
            byte[] badError = GOLDEN.clone();
            ByteBuffer.wrap(badError).order(ByteOrder.BIG_ENDIAN).putShort(12, errorId);
            assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(new ByteArrayInputStream(badError)));
        }
    }

    @Test
    @DisplayName("Propagates the same underlying IO exception from read and write")
    void propagatesTheSameUnderlyingIoExceptionFromReadAndWrite() {
        IOException readFailure = new IOException("read failed");
        InputStream failedInput = new InputStream() {
            @Override public int read() throws IOException { throw readFailure; }
        };
        assertSame(readFailure, assertThrows(IOException.class, () -> FrameCodec.read(failedInput)));

        IOException writeFailure = new IOException("write failed");
        OutputStream failedOutput = new OutputStream() {
            @Override public void write(int value) throws IOException { throw writeFailure; }
        };
        assertSame(writeFailure, assertThrows(IOException.class,
                () -> FrameCodec.write(failedOutput, new Frame(Api.METADATA, 1, ErrorCode.NONE, new byte[0]))));
    }

    private static InputStream fragmented(byte[] wire, int maxChunk) {
        return new ByteArrayInputStream(wire) {
            @Override
            public synchronized int read(byte[] bytes, int offset, int length) {
                return super.read(bytes, offset, Math.min(length, maxChunk));
            }
        };
    }

    private static byte[] encode(Frame frame) throws IOException {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.write(output, frame);
        return output.toByteArray();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }

    private static byte[] hex(String value) {
        return HexFormat.of().parseHex(value);
    }

    private static void assertFrameEquals(Frame expected, Frame actual) {
        assertEquals(expected.api(), actual.api());
        assertEquals(expected.correlationId(), actual.correlationId());
        assertEquals(expected.error(), actual.error());
        assertArrayEquals(expected.payload(), actual.payload());
    }
    private record HeaderWireCase(short api, ErrorCode error, String goldenHex) { }
}
