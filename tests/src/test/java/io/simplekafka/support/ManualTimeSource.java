package io.simplekafka.support;

import java.util.concurrent.atomic.AtomicLong;

/** Controllable millisecond clock for deterministic expiration tests. */
public final class ManualTimeSource implements TimeSource {
    private final AtomicLong nowMillis;

    public ManualTimeSource(long initialMillis) {
        if (initialMillis < 0) throw new IllegalArgumentException("initialMillis must be nonnegative");
        nowMillis = new AtomicLong(initialMillis);
    }

    @Override
    public long nowMillis() {
        return nowMillis.get();
    }

    public void setMillis(long millis) {
        if (millis < 0) throw new IllegalArgumentException("millis must be nonnegative");
        nowMillis.set(millis);
    }

    public long advanceMillis(long delta) {
        if (delta < 0) throw new IllegalArgumentException("delta must be nonnegative");
        return nowMillis.updateAndGet(current -> Math.addExact(current, delta));
    }
}
