package io.simplekafka.support;

@FunctionalInterface
public interface TimeSource {
    long nowMillis();
}
