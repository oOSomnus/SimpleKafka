package io.simplekafka.cluster;

/**
 * Per-broker mirror of a replica's epoch, leadership, and online status.
 *
 * <p>Role fields are volatile so updates are visible to readers on other threads.
 */
public final class ReplicaState {
    private final int brokerId;
    private volatile int epoch;
    private volatile boolean leader;
    private volatile boolean online;

    /**
     * Creates the role and liveness mirror for one broker's replica.
     *
     * @param brokerId broker that owns this state
     * @param epoch current leader epoch
     * @param leader whether this broker is the partition leader
     * @param online whether this broker is online
     * @throws IllegalArgumentException if {@code brokerId} or {@code epoch} is negative
     */
    public ReplicaState(int brokerId, int epoch, boolean leader, boolean online) {
        if (brokerId < 0 || epoch < 0)
            throw new IllegalArgumentException("negative broker id or epoch");
        this.brokerId = brokerId;
        this.epoch = epoch;
        this.leader = leader;
        this.online = online;
    }

    /**
     * Returns the broker identifier associated with this replica state.
     *
     * @return the broker identifier
     */
    public int brokerId() {
        return brokerId;
    }

    /**
     * Returns the epoch currently mirrored for this broker.
     *
     * @return the mirrored leader epoch
     */
    public int epoch() {
        return epoch;
    }

    /**
     * Reports whether this broker is currently the replica leader.
     *
     * @return {@code true} if this replica is marked as the leader
     */
    public boolean isLeader() {
        return leader;
    }

    /**
     * Reports whether this broker is currently marked online.
     *
     * @return {@code true} if this replica is marked online
     */
    public boolean isOnline() {
        return online;
    }

    /**
     * Replaces the mirrored epoch, leader flag, and online flag.
     *
     * @param epoch epoch to store
     * @param leader whether the broker is the current leader
     * @param online whether the broker is online
     */
    public synchronized void update(int epoch, boolean leader, boolean online) {
        this.epoch = epoch;
        this.leader = leader;
        this.online = online;
    }
}
