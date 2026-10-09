package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.model.BusinessEvent;
import io.simplekafka.storage.PartitionLog;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;

/** Step 31: one-log durable deduplication ledger for application-level business effects. */
public final class DurableEffectStore implements AutoCloseable {
    private static final int MAX_RECORD_BYTES = 1_048_580;

    private final PartitionLog log;
    private final Map<String, BusinessEvent> seen = new HashMap<>();
    private final Map<String, Long> balances = new HashMap<>();
    private boolean failed;
    private boolean closed;

    public DurableEffectStore(Path directory) {
        log = new PartitionLog(directory, 1_048_576, 1);
        try {
            recover();
        } catch (RuntimeException failure) {
            try {
                log.close();
            } catch (RuntimeException closeFailure) {
                failure.addSuppressed(closeFailure);
            }
            throw failure;
        }
    }

    /** Persists one event atomically with its deduplication identity and balance update. */
    public synchronized boolean apply(BusinessEvent event) {
        throw new ExerciseNotImplementedException(31, "DurableEffectStore.apply");
    }

    /** Returns the current balance, or zero for an account with no recorded events. */
    public synchronized long balance(String account) {
        ensureUsable();
        requireFullLedger();
        validateAccount(account);
        return balances.getOrDefault(account, 0L);
    }

    /** Rebuilds event identity and account balances from the complete retained ledger. */
    public synchronized void recover() {
        throw new ExerciseNotImplementedException(31, "DurableEffectStore.recover");
    }

    private void ensureUsable() {
        if (closed || failed)
            throw new CourseException(
                    ErrorCode.STORAGE_ERROR, "durable effect store is unavailable");
    }

    private void requireFullLedger() {
        if (log.logStartOffset() != 0) {
            failed = true;
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "durable effect ledger was truncated");
        }
    }

    private static void validateAccount(String account) {
        if (account == null || account.isEmpty()) throw invalid("account must be nonempty");
        try {
            int bytes =
                    StandardCharsets.UTF_8
                            .newEncoder()
                            .onMalformedInput(CodingErrorAction.REPORT)
                            .onUnmappableCharacter(CodingErrorAction.REPORT)
                            .encode(CharBuffer.wrap(account))
                            .remaining();
            if (bytes > 255) throw invalid("account exceeds 255 UTF-8 bytes");
        } catch (CharacterCodingException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "account is not valid UTF-8", exception);
        }
    }

    private static CourseException invalid(String message) {
        return new CourseException(ErrorCode.INVALID_REQUEST, message);
    }

    private static boolean isUncertainStorageFailure(RuntimeException exception) {
        return exception instanceof CourseException courseException
                && (courseException.code() == ErrorCode.STORAGE_ERROR
                        || courseException.code() == ErrorCode.CORRUPT_RECORD);
    }

    @Override
    public synchronized void close() {
        if (closed) return;
        closed = true;
        log.close();
    }
}
