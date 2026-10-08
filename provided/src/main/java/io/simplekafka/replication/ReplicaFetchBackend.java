package io.simplekafka.replication;

import io.simplekafka.protocol.Messages;

/** Backend hook for serving replica fetches beyond the consumer high watermark. */
public interface ReplicaFetchBackend {
    Messages.ReplicaFetchBody fetchForReplica(
            int brokerId,
            int epoch,
            long fetchOffset,
            int maxRecords,
            int maxBytes,
            boolean recoveryRead);
}
