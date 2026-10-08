package io.simplekafka.support;

/** System wall-clock implementation of {@link TimeSource}, exposed as a singleton enum. */
public enum SystemTimeSource implements TimeSource {
    INSTANCE;

    /**
     * Returns the current wall-clock time in milliseconds since the epoch.
     *
     * @return current system time in milliseconds; this value is not guaranteed to be monotonic
     */
    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
