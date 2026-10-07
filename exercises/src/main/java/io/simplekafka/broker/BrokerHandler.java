package io.simplekafka.broker;

import io.simplekafka.ExerciseNotImplementedException;
import io.simplekafka.cluster.ClusterAuthority;
import io.simplekafka.group.GroupCoordinator;
import io.simplekafka.group.OffsetStore;
import io.simplekafka.protocol.Messages;
import java.util.Objects;

/** Request dispatch shared by single and replicated brokers. */
public final class BrokerHandler {
    private final PartitionCatalog catalog;
    private final ClusterAuthority authority;
    private final GroupCoordinator groups;
    private final OffsetStore offsets;

    public BrokerHandler(PartitionCatalog catalog, ClusterAuthority authority,
                         GroupCoordinator groups, OffsetStore offsets) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.authority = authority;
        this.groups = groups;
        this.offsets = offsets;
    }

    /** Step 12; dispatch broker requests and return precise protocol error replies. */
    public Messages.Reply handle(Messages.Request request) {
        throw new ExerciseNotImplementedException(12, "BrokerHandler.handle");
    }
}
