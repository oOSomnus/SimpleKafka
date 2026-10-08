package io.simplekafka.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Positional channel operations that tolerate partial reads and writes. */
public final class ChannelIO {
    private ChannelIO() {}

    /**
     * Fills {@code destination} using positional reads beginning at {@code position}.
     *
     * <p>Each read advances the destination buffer position, but does not change the channel's
     * position or close the channel. The operation retries partial and zero-byte reads.
     *
     * @param channel channel to read from; it remains open
     * @param destination buffer to fill; its position advances as bytes are read
     * @param position nonnegative file position at which to begin reading
     * @throws IllegalArgumentException if {@code position} is negative
     * @throws NullPointerException if {@code destination} is {@code null}, or if {@code channel} is
     *     {@code null} while the destination has remaining bytes
     * @throws EOFException if end of file is reached before the destination is full
     * @throws IOException if a channel read fails
     */
    public static void readFully(FileChannel channel, ByteBuffer destination, long position)
            throws IOException {
        if (position < 0) throw new IllegalArgumentException("position must be nonnegative");
        long current = position;
        while (destination.hasRemaining()) {
            int read = channel.read(destination, current);
            if (read < 0) throw new EOFException("unexpected end of file");
            if (read == 0) {
                Thread.onSpinWait();
                continue;
            }
            current += read;
        }
    }

    /**
     * Writes all remaining bytes of {@code source} using positional writes beginning at {@code
     * position}.
     *
     * <p>Each write advances the source buffer position, but does not change the channel's position
     * or close the channel. The operation retries partial and zero-byte writes.
     *
     * @param channel channel to write to; it remains open
     * @param source buffer whose remaining bytes are written; its position advances as bytes are
     *     written
     * @param position nonnegative file position at which to begin writing
     * @throws IllegalArgumentException if {@code position} is negative
     * @throws NullPointerException if {@code source} is {@code null}, or if {@code channel} is
     *     {@code null} while the source has remaining bytes
     * @throws IOException if a channel write fails
     */
    public static void writeFully(FileChannel channel, ByteBuffer source, long position)
            throws IOException {
        if (position < 0) throw new IllegalArgumentException("position must be nonnegative");
        long current = position;
        while (source.hasRemaining()) {
            int written = channel.write(source, current);
            if (written == 0) {
                Thread.onSpinWait();
                continue;
            }
            current += written;
        }
    }
}
