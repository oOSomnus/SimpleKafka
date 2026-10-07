package io.simplekafka.course;

import io.simplekafka.ErrorCode;
import io.simplekafka.protocol.Api;
import io.simplekafka.protocol.Frame;
import io.simplekafka.protocol.FrameCodec;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;
import java.util.Arrays;
import org.junit.jupiter.api.Test;
import static io.simplekafka.support.TestSupport.assertCode;
import static org.junit.jupiter.api.Assertions.assertArrayEquals;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

class Step11Test {
    @Test
    void readsBackToBackFramesFromAStreamThatDeliversOneByteAtATime() throws Exception {
        Frame first = new Frame(Api.METADATA, 41, ErrorCode.NONE, new byte[]{1, 2, 3});
        Frame second = new Frame(Api.FETCH, 42, ErrorCode.REQUEST_TIMEOUT, new byte[]{9, 8});
        byte[] wire = concat(encode(first), encode(second));
        ByteArrayInputStream oneByteReads = new ByteArrayInputStream(wire) {
            @Override
            public synchronized int read(byte[] bytes, int offset, int length) {
                return super.read(bytes, offset, Math.min(length, 1));
            }
        };

        assertFrameEquals(first, FrameCodec.read(oneByteReads));
        assertFrameEquals(second, FrameCodec.read(oneByteReads));
        assertNull(FrameCodec.read(oneByteReads));
        assertNull(FrameCodec.read(new ByteArrayInputStream(new byte[0])));
    }

    @Test
    void rejectsTruncatedFramesUnsupportedVersionAndUnknownApi() throws Exception {
        byte[] complete = encode(new Frame(Api.METADATA, 7, ErrorCode.NONE, new byte[]{3, 4}));
        assertCode(ErrorCode.INVALID_REQUEST,
                () -> FrameCodec.read(new ByteArrayInputStream(Arrays.copyOf(complete, complete.length - 1))));

        byte[] badVersion = complete.clone();
        ByteBuffer.wrap(badVersion).order(ByteOrder.BIG_ENDIAN).putShort(4, (short) 2);
        assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(new ByteArrayInputStream(badVersion)));

        byte[] badApi = complete.clone();
        ByteBuffer.wrap(badApi).order(ByteOrder.BIG_ENDIAN).putShort(6, (short) 32_000);
        assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(new ByteArrayInputStream(badApi)));
        assertCode(ErrorCode.INVALID_REQUEST,
                () -> FrameCodec.write(new ByteArrayOutputStream(), new Frame((short) 32_000, 1, ErrorCode.NONE, new byte[0])));
    }

    @Test
    void validatesFrameLengthBeforeReadingAnyPayload() throws Exception {
        byte[] bytes = ByteBuffer.allocate(5).order(ByteOrder.BIG_ENDIAN)
                .putInt(8_388_609).put((byte) 99).array();
        ByteArrayInputStream input = new ByteArrayInputStream(bytes);
        assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(input));
        assertEquals(1, input.available(), "the byte after an invalid length header is not consumed");

        byte[] tooShort = ByteBuffer.allocate(4).order(ByteOrder.BIG_ENDIAN).putInt(9).array();
        assertCode(ErrorCode.INVALID_REQUEST, () -> FrameCodec.read(new ByteArrayInputStream(tooShort)));
    }

    private static byte[] encode(Frame frame) throws Exception {
        ByteArrayOutputStream output = new ByteArrayOutputStream();
        FrameCodec.write(output, frame);
        return output.toByteArray();
    }

    private static byte[] concat(byte[] first, byte[] second) {
        byte[] joined = Arrays.copyOf(first, first.length + second.length);
        System.arraycopy(second, 0, joined, first.length, second.length);
        return joined;
    }

    private static void assertFrameEquals(Frame expected, Frame actual) {
        assertEquals(expected.api(), actual.api());
        assertEquals(expected.correlationId(), actual.correlationId());
        assertEquals(expected.error(), actual.error());
        assertArrayEquals(expected.payload(), actual.payload());
    }
}
