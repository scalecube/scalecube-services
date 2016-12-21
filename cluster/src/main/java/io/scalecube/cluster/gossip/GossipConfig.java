package io.scalecube.cluster.gossip;

public final class GossipConfig {

  public static final long DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_FANOUT = 3;
  public static final int DEFAULT_GOSSIP_FACTOR = 2;

  private final long gossipInterval;
  private final int gossipFanout;
  private final int gossipFactor;

  private GossipConfig(Builder builder) {
    this.gossipFanout = builder.gossipFanout;
    this.gossipInterval = builder.gossipInterval;
    this.gossipFactor = builder.gossipFactor;
  }

  public static GossipConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getGossipFanout() {
    return gossipFanout;
  }

  public long getGossipInterval() {
    return gossipInterval;
  }

  public int getGossipFactor() {
    return gossipFactor;
  }

  @Override
  public String toString() {
    return "GossipConfig{gossipInterval=" + gossipInterval
        + ", gossipFanout=" + gossipFanout
        + ", gossipFactor=" + gossipFactor
        + '}';
  }

  public static final class Builder {

    private long gossipInterval = DEFAULT_GOSSIP_INTERVAL;
    private int gossipFanout = DEFAULT_GOSSIP_FANOUT;
    private int gossipFactor = DEFAULT_GOSSIP_FACTOR;

    private Builder() {}

    public Builder gossipInterval(long gossipInterval) {
      this.gossipInterval = gossipInterval;
      return this;
    }

    public Builder gossipFanout(int gossipFanout) {
      this.gossipFanout = gossipFanout;
      return this;
    }

    public Builder gossipFactor(int gossipFactor) {
      this.gossipFactor = gossipFactor;
      return this;
    }

    public GossipConfig build() {
      return new GossipConfig(this);
    }
  }
}
