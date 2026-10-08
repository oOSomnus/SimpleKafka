package io.simplekafka.model;

public record IndexEntry(long offset, long position) {
    public IndexEntry {
        if (offset < 0 || position < 0)
            throw new IllegalArgumentException("index values must be nonnegative");
    }
}
