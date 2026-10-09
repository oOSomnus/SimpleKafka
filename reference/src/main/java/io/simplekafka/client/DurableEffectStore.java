package io.simplekafka.client;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.AppendResult;
import io.simplekafka.model.BusinessEvent;
import io.simplekafka.model.LogRecord;
import io.simplekafka.model.RecordData;
import io.simplekafka.protocol.MessageCodec;
import io.simplekafka.storage.PartitionLog;

import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.CodingErrorAction;
import java.nio.charset.StandardCharsets;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** One-log durable deduplication ledger for application-level business effects. */
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
        ensureUsable();
        requireFullLedger();
        if (event == null) throw invalid("business event is null");
        BusinessEvent previous = seen.get(event.eventId());
        if (previous != null) {
            if (!previous.equals(event))
                throw invalid("business event id was reused with different content");
            return false;
        }
        long current = balances.getOrDefault(event.account(), 0L);
        long next;
        try {
            next = Math.addExact(current, event.delta());
        } catch (ArithmeticException exception) {
            throw new CourseException(
                    ErrorCode.INVALID_REQUEST, "account balance overflows", exception);
        }
        byte[] payload = MessageCodec.encodeBusinessEvent(event);
        AppendResult appended;
        try {
            appended = log.append(List.of(new RecordData(null, payload, 0)));
        } catch (RuntimeException exception) {
            if (isUncertainStorageFailure(exception)) failed = true;
            throw exception;
        }
        if (appended.nextOffset() - appended.firstOffset() != 1) {
            failed = true;
            throw new CourseException(
                    ErrorCode.CORRUPT_RECORD, "ledger append returned an invalid range");
        }
        seen.put(event.eventId(), event);
        balances.put(event.account(), next);
        return true;
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
        ensureUsable();
        seen.clear();
        balances.clear();
        try {
            requireFullLedger();
            long offset = 0;
            long leo = log.logEndOffset();
            while (offset < leo) {
                List<LogRecord> page = log.read(offset, 1, MAX_RECORD_BYTES);
                if (page.size() != 1 || page.getFirst().offset() != offset)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD, "ledger replay made no progress");
                LogRecord record = page.getFirst();
                if (record.data().key() != null
                        || record.data().timestamp() != 0
                        || record.data().producerStamp() != null)
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD, "ledger record metadata is invalid");
                BusinessEvent event;
                try {
                    event = MessageCodec.decodeBusinessEvent(record.data().value());
                } catch (CourseException exception) {
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD,
                            "ledger contains an invalid business event",
                            exception);
                }
                BusinessEvent previous = seen.get(event.eventId());
                if (previous != null) {
                    if (!previous.equals(event))
                        throw new CourseException(
                                ErrorCode.CORRUPT_RECORD,
                                "ledger event id is associated with conflicting content");
                } else {
                    long current = balances.getOrDefault(event.account(), 0L);
                    long next;
                    try {
                        next = Math.addExact(current, event.delta());
                    } catch (ArithmeticException exception) {
                        throw new CourseException(
                                ErrorCode.CORRUPT_RECORD,
                                "ledger account balance overflows during recovery",
                                exception);
                    }
                    seen.put(event.eventId(), event);
                    balances.put(event.account(), next);
                }
                try {
                    offset = Math.addExact(offset, 1L);
                } catch (ArithmeticException exception) {
                    throw new CourseException(
                            ErrorCode.CORRUPT_RECORD, "ledger offset overflows", exception);
                }
            }
        } catch (RuntimeException exception) {
            seen.clear();
            balances.clear();
            failed = true;
            throw exception;
        }
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
