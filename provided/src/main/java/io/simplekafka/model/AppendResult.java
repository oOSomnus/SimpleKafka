package io.simplekafka.model;

public record AppendResult(long firstOffset, long nextOffset) {
    public AppendResult {
        if (firstOffset < 0 || nextOffset < firstOffset) throw new IllegalArgumentException("invalid append range");
    }
    public int count() { return Math.toIntExact(nextOffset - firstOffset); }
}
