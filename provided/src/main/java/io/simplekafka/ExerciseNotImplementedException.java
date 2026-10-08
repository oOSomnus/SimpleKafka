package io.simplekafka;

/** Signals that a course exercise method has not yet been implemented. */
public final class ExerciseNotImplementedException extends RuntimeException {
    private final int step;
    private final String method;

    /**
     * Creates an exercise-not-implemented signal and formats its diagnostic message.
     *
     * @param step course step number associated with the exercise
     * @param method name of the exercise method
     */
    public ExerciseNotImplementedException(int step, String method) {
        super("Step %02d not implemented: %s".formatted(step, method));
        this.step = step;
        this.method = method;
    }

    /**
     * Returns the course step number associated with this exercise.
     *
     * @return the step number supplied at construction
     */
    public int step() {
        return step;
    }

    /**
     * Returns the exercise method name supplied at construction.
     *
     * @return the method name supplied at construction
     */
    public String method() {
        return method;
    }
}
