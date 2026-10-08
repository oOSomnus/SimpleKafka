package io.simplekafka.replication;

import io.simplekafka.protocol.Messages;

/** Backend hook for serving replica fetches beyond the consumer high watermark. */
public interface ReplicaFetchBackend {
    /**
     * Fetches records for a replica through the leader's log end, including records at or above the
     * consumer high watermark.
     *
     * <p>A fetch at the leader log end returns no records. Recovery reads use the same range and
     * limits but do not report follower progress or change ISR membership or the high watermark.
     *
     * @param brokerId requesting follower broker ID; it must be assigned to the partition and
     *     online
     * @param epoch current leader epoch expected by the requester
     * @param fetchOffset first requested offset in the inclusive range from log start to leader log
     *     end
     * @param maxRecords maximum number of records; must be positive
     * @param maxBytes maximum byte budget, measured using complete on-disk records including each
     *     four-byte length prefix; must be positive
     * @param recoveryRead whether this is a recovery reconciliation read; it does not change the
     *     returned range
     * @return fetched records and the leader replication boundaries
     * @throws io.simplekafka.CourseException if arguments are invalid or the requester is not an
     *     assigned replica ({@link io.simplekafka.ErrorCode#INVALID_REQUEST}), the requester is
     *     offline or this backend is not served by the online leader ({@link
     *     io.simplekafka.ErrorCode#NOT_LEADER}), the epoch is stale ({@link
     *     io.simplekafka.ErrorCode#FENCED_EPOCH}), the offset is outside the retained leader log
     *     range ({@link io.simplekafka.ErrorCode#OFFSET_OUT_OF_RANGE}), or storage fails / the log
     *     is corrupt ({@link io.simplekafka.ErrorCode#STORAGE_ERROR} or {@link
     *     io.simplekafka.ErrorCode#CORRUPT_RECORD})
     */
    Messages.ReplicaFetchBody fetchForReplica(
            int brokerId,
            int epoch,
            long fetchOffset,
            int maxRecords,
            int maxBytes,
            boolean recoveryRead);
}
