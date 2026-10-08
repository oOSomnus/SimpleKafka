package io.simplekafka.storage;

import java.io.EOFException;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.channels.FileChannel;

/** Positional channel operations that tolerate partial reads and writes. */
public final class ChannelIO {
    private ChannelIO() {}

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
