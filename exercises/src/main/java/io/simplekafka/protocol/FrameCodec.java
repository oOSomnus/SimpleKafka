package io.simplekafka.protocol;

import io.simplekafka.ExerciseNotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Step 11: encode and decode one course-protocol frame. */
public final class FrameCodec {
    private FrameCodec() {}

    /**
     * Step 11 contract: write one bounded, big-endian frame; see Step11Test and the network
     * chapter.
     */
    public static void write(OutputStream output, Frame frame) throws IOException {
        throw new ExerciseNotImplementedException(11, "FrameCodec.write");
    }

    /**
     * Step 11 contract: read one frame, return null only on clean frame-boundary EOF; see
     * Step11Test.
     */
    public static Frame read(InputStream input) throws IOException {
        throw new ExerciseNotImplementedException(11, "FrameCodec.read");
    }
}
