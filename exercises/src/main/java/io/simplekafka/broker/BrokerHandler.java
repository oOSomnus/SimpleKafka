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

    /**
     * Creates a request handler for the supplied catalog and optional cluster/group services.
     *
     * @param catalog local partition catalog, required
     * @param authority cluster authority for replicated request dispatch, or {@code null} when
     *     replication is not configured
     * @param groups group coordinator for group request dispatch, or {@code null} when groups are
     *     not configured
     * @param offsets durable offset store for offset request dispatch, or {@code null} when offset
     *     storage is not configured
     * @throws NullPointerException if {@code catalog} is null
     */
    public BrokerHandler(
            PartitionCatalog catalog,
            ClusterAuthority authority,
            GroupCoordinator groups,
            OffsetStore offsets) {
        this.catalog = Objects.requireNonNull(catalog, "catalog");
        this.authority = authority;
        this.groups = groups;
        this.offsets = offsets;
    }

    /**
     * Step 12: dispatch supported broker request types and map failures to protocol replies. Course
     * exceptions preserve their code and message; argument failures map to {@link
     * io.simplekafka.ErrorCode#INVALID_REQUEST}, and other runtime failures map to {@link
     * io.simplekafka.ErrorCode#STORAGE_ERROR}. Null or unsupported requests, missing optional
     * group/offset services, empty produce batches, nonpositive fetch limits, and negative produce
     * timeouts return INVALID_REQUEST; unknown partitions, stale epochs, nonleader access, and
     * out-of-range fetch offsets return their corresponding error replies, not thrown exceptions.
     * The handler grows with offsets, groups, token fencing, and replica fetch (Steps 15 and
     * 18–21); see Step12Test and the course contracts.
     *
     * @param request request to dispatch
     * @return a success reply or an error reply carrying the mapped error code and message
     * @throws ExerciseNotImplementedException while the Step 12 exercise method is a skeleton
     */
    public Messages.Reply handle(Messages.Request request) {
        throw new ExerciseNotImplementedException(12, "BrokerHandler.handle");
    }
}
