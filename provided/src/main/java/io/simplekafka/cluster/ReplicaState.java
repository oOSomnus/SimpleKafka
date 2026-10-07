package io.simplekafka.cluster;

public final class ReplicaState {
    private final int brokerId;
    private volatile int epoch;
    private volatile boolean leader;
    private volatile boolean online;
    public ReplicaState(int brokerId, int epoch, boolean leader, boolean online) {
        if (brokerId < 0 || epoch < 0) throw new IllegalArgumentException("negative broker id or epoch");
        this.brokerId = brokerId; this.epoch = epoch; this.leader = leader; this.online = online;
    }
    public int brokerId() { return brokerId; }
    public int epoch() { return epoch; }
    public boolean isLeader() { return leader; }
    public boolean isOnline() { return online; }
    public void update(int epoch, boolean leader, boolean online) {
        this.epoch = epoch; this.leader = leader; this.online = online;
    }
}
