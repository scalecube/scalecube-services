package io.scalecube.cluster.gossip;

public final class GossipConfig {

  public static final long DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_FANOUT = 3;
  public static final int DEFAULT_GOSSIP_REPEAT_MULTIPLIER = 2;

  private final long gossipInterval;
  private final int gossipFanout;
  private final int gossipRepeatMultiplier;

  private GossipConfig(Builder builder) {
    this.gossipFanout = builder.gossipFanout;
    this.gossipInterval = builder.gossipInterval;
    this.gossipRepeatMultiplier = builder.gossipRepeatMultiplier;
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

  public int getGossipRepeatMultiplier() {
    return gossipRepeatMultiplier;
  }

  @Override
  public String toString() {
    return "GossipConfig{gossipInterval=" + gossipInterval
        + ", gossipFanout=" + gossipFanout
        + ", gossipRepeatMultiplier=" + gossipRepeatMultiplier
        + '}';
  }

  public static final class Builder {

    private long gossipInterval = DEFAULT_GOSSIP_INTERVAL;
    private int gossipFanout = DEFAULT_GOSSIP_FANOUT;
    private int gossipRepeatMultiplier = DEFAULT_GOSSIP_REPEAT_MULTIPLIER;

    private Builder() {}

    public Builder gossipInterval(long gossipInterval) {
      this.gossipInterval = gossipInterval;
      return this;
    }

    public Builder gossipFanout(int gossipFanout) {
      this.gossipFanout = gossipFanout;
      return this;
    }

    public Builder gossipRepeatMultiplier(int gossipRepeatMultiplier) {
      this.gossipRepeatMultiplier = gossipRepeatMultiplier;
      return this;
    }

    public GossipConfig build() {
      return new GossipConfig(this);
    }
  }
}
