package io.simplekafka.protocol;

import io.simplekafka.ExerciseNotImplementedException;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;

/** Step 11: encode and decode one course-protocol frame. */
public final class FrameCodec {
    private FrameCodec() {}

    /**
     * Step 11 contract: write one bounded, big-endian version 1 course-protocol frame in the form
     * {@code int32 length | int16 version | int16 API | int32 correlation ID | int16 error |
     * payload}; length excludes its own four-byte prefix. See Step11Test and the network chapter.
     *
     * @param output stream to receive the frame
     * @param frame frame to encode
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     *     if either argument is null, the API is unknown, or the frame length is outside {@code
     *     [10, 8_388_608]}
     * @throws IOException if writing the frame fails
     * @throws ExerciseNotImplementedException while the Step 11 exercise method is a skeleton
     */
    public static void write(OutputStream output, Frame frame) throws IOException {
        throw new ExerciseNotImplementedException(11, "FrameCodec.write");
    }

    /**
     * Step 11 contract: read one course-protocol frame and return null only on clean frame-boundary
     * EOF; see Step11Test.
     *
     * @param input stream to read
     * @return the next frame, or {@code null} when EOF occurs before any byte of a new frame
     * @throws io.simplekafka.CourseException with {@link io.simplekafka.ErrorCode#INVALID_REQUEST}
     *     for a null stream, a truncated frame, length outside {@code [10, 8_388_608]}, unsupported
     *     version, unknown API, or unknown error code
     * @throws IOException if reading the stream fails
     * @throws ExerciseNotImplementedException while the Step 11 exercise method is a skeleton
     */
    public static Frame read(InputStream input) throws IOException {
        throw new ExerciseNotImplementedException(11, "FrameCodec.read");
    }
}
