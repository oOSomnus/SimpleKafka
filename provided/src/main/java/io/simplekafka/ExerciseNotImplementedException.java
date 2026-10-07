package io.simplekafka;

public final class ExerciseNotImplementedException extends RuntimeException {
    private final int step;
    private final String method;
    public ExerciseNotImplementedException(int step, String method) {
        super("Step %02d not implemented: %s".formatted(step, method));
        this.step = step;
        this.method = method;
    }
    public int step() { return step; }
    public String method() { return method; }
}
