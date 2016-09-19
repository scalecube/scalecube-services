package io.scalecube.cluster.gossip;

public final class GossipConfig {

  public static final int DEFAULT_GOSSIP_INTERVAL = 200;
  public static final int DEFAULT_GOSSIP_FANOUT = 3;

  private final int gossipInterval;
  private final int gossipFanout;

  private GossipConfig(Builder builder) {
    this.gossipFanout = builder.gossipFanout;
    this.gossipInterval = builder.gossipInterval;
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

  public int getGossipInterval() {
    return gossipInterval;
  }

  @Override
  public String toString() {
    return "GossipConfig{gossipInterval=" + gossipInterval + ", gossipFanout=" + gossipFanout + '}';
  }

  public static final class Builder {

    private int gossipInterval = DEFAULT_GOSSIP_INTERVAL;
    private int gossipFanout = DEFAULT_GOSSIP_FANOUT;

    private Builder() {}

    public Builder gossipInterval(int gossipInterval) {
      this.gossipInterval = gossipInterval;
      return this;
    }

    public Builder gossipFanout(int gossipFanout) {
      this.gossipFanout = gossipFanout;
      return this;
    }

    public GossipConfig build() {
      return new GossipConfig(this);
    }
  }
}
