package io.scalecube.cluster.gossip;

public final class GossipProtocolSettings {

  public static final GossipProtocolSettings DEFAULT = builder().build();

  public static final int DEFAULT_MAX_GOSSIP_SENT = 2;
  public static final int DEFAULT_GOSSIP_TIME = 300;
  public static final int DEFAULT_MAX_ENDPOINTS_TO_SELECT = 3;

  private final int maxGossipSent;
  private final int gossipTime;
  private final int maxEndpointsToSelect;

  private GossipProtocolSettings(Builder builder) {
    this.maxGossipSent = builder.maxGossipSent;
    this.gossipTime = builder.gossipTime;
    this.maxEndpointsToSelect = builder.maxEndpointsToSelect;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getMaxGossipSent() {
    return maxGossipSent;
  }

  public int getGossipTime() {
    return gossipTime;
  }

  public int getMaxEndpointsToSelect() {
    return maxEndpointsToSelect;
  }

  @Override
  public String toString() {
    return "GossipProtocolSettings{maxGossipSent=" + maxGossipSent
        + ", gossipTime=" + gossipTime
        + ", maxEndpointsToSelect=" + maxEndpointsToSelect
        + '}';
  }

  public static final class Builder {

    private int maxGossipSent = DEFAULT_MAX_GOSSIP_SENT;
    private int gossipTime = DEFAULT_GOSSIP_TIME;
    private int maxEndpointsToSelect = DEFAULT_MAX_ENDPOINTS_TO_SELECT;

    private Builder() {}

    public Builder maxGossipSent(int maxGossipSent) {
      this.maxGossipSent = maxGossipSent;
      return this;
    }

    public Builder gossipTime(int gossipTime) {
      this.gossipTime = gossipTime;
      return this;
    }

    public Builder maxEndpointsToSelect(int maxEndpointsToSelect) {
      this.maxEndpointsToSelect = maxEndpointsToSelect;
      return this;
    }

    public GossipProtocolSettings build() {
      return new GossipProtocolSettings(this);
    }
  }
}
