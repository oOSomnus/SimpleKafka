package io.simplekafka.support;

public enum SystemTimeSource implements TimeSource {
    INSTANCE;

    @Override
    public long nowMillis() {
        return System.currentTimeMillis();
    }
}
