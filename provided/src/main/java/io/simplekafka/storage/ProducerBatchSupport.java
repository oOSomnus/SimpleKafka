package io.simplekafka.storage;

import io.simplekafka.CourseException;
import io.simplekafka.ErrorCode;
import io.simplekafka.model.ProducerStamp;
import io.simplekafka.model.RecordData;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Shared storage checks for batch boundaries in a stamped record stream. */
public final class ProducerBatchSupport {
    private ProducerBatchSupport() {}

    /** Validates stamped batch groups before the partition log begins a persistent mutation. */
    public static void validateAppend(ProducerStamp tail, List<RecordData> incoming) {
        if (incoming == null || incoming.isEmpty())
            throw new CourseException(ErrorCode.INVALID_REQUEST, "append batch must not be empty");
        boolean tailIncomplete = isIncomplete(tail);
        ProducerStamp current = tailIncomplete ? tail : null;
        Map<Long, ProducerStamp> lastCompleteByProducer = new HashMap<>();
        if (tail != null && !tailIncomplete) lastCompleteByProducer.put(tail.producerId(), tail);
        if (tailIncomplete) {
            ProducerStamp first = stamp(incoming.getFirst());
            if (!sameBatch(tail, first) || first.batchIndex() != tail.batchIndex() + 1)
                throw new CourseException(
                        ErrorCode.INCOMPLETE_BATCH,
                        "the incomplete producer batch must be continued before another append");
        }

        for (int index = 0; index < incoming.size(); index++) {
            ProducerStamp next = stamp(incoming.get(index));
            if (next == null) {
                if (current != null)
                    throw corrupt("producer batch is interrupted by an ordinary record");
                continue;
            }
            if (current != null) {
                if (!sameBatch(current, next))
                    throw corrupt("producer batch metadata changes before the batch is complete");
                if (next.batchIndex() != current.batchIndex() + 1)
                    throw corrupt("producer batch indices are not contiguous");
                current = next;
                if (next.batchIndex() == next.batchSize() - 1) {
                    lastCompleteByProducer.put(next.producerId(), next);
                    current = null;
                }
                continue;
            }
            if (next.batchIndex() != 0)
                throw corrupt("producer batch does not begin at index zero");
            ProducerStamp previous = lastCompleteByProducer.get(next.producerId());
            if (previous != null) {
                if (next.producerEpoch() < previous.producerEpoch())
                    throw corrupt("producer epoch moves backwards in the log");
                if (next.producerEpoch() == previous.producerEpoch()) {
                    long expectedSequence;
                    try {
                        expectedSequence =
                                Math.addExact(previous.firstSequence(), previous.batchSize());
                    } catch (ArithmeticException exception) {
                        throw corrupt("producer sequence range overflows", exception);
                    }
                    if (next.firstSequence() != expectedSequence)
                        throw corrupt("producer sequence is discontinuous in the log");
                } else if (next.firstSequence() != 0) {
                    throw corrupt("a new producer epoch must begin at sequence zero");
                }
            }
            current = next;
            if (next.batchIndex() == next.batchSize() - 1) {
                lastCompleteByProducer.put(next.producerId(), next);
                current = null;
            }
        }
    }

    private static ProducerStamp stamp(RecordData data) {
        if (data == null)
            throw new CourseException(ErrorCode.INVALID_REQUEST, "record data is null");
        return data.producerStamp();
    }

    private static boolean isIncomplete(ProducerStamp stamp) {
        return stamp != null && stamp.batchIndex() < stamp.batchSize() - 1;
    }

    private static boolean sameBatch(ProducerStamp left, ProducerStamp right) {
        return right != null
                && left.producerId() == right.producerId()
                && left.producerEpoch() == right.producerEpoch()
                && left.firstSequence() == right.firstSequence()
                && left.batchSize() == right.batchSize()
                && java.util.Arrays.equals(left.batchHash(), right.batchHash());
    }

    private static CourseException corrupt(String message) {
        return new CourseException(ErrorCode.CORRUPT_RECORD, message);
    }

    private static CourseException corrupt(String message, Throwable cause) {
        return new CourseException(ErrorCode.CORRUPT_RECORD, message, cause);
    }
}
