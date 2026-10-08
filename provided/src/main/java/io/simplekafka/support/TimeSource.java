package io.simplekafka.support;

/**
 * Supplies millisecond timestamps for group expiry and replication timing.
 *
 * <p>Implementations are not required to provide a monotonic clock.
 */
@FunctionalInterface
public interface TimeSource {
    /**
     * Returns a timestamp in milliseconds from this clock.
     *
     * @return current clock value in milliseconds
     */
    long nowMillis();
}
