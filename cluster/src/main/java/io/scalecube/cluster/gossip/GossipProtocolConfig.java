package io.scalecube.cluster.gossip;

public final class GossipProtocolConfig {

  public static final GossipProtocolConfig DEFAULT = builder().build();

  public static final int DEFAULT_MAX_GOSSIP_SENT = 2;
  public static final int DEFAULT_GOSSIP_TIME = 300;
  public static final int DEFAULT_MAX_MEMBERS_TO_SELECT = 3;

  private final int maxGossipSent;
  private final int gossipTime;
  private final int maxMembersToSelect;

  private GossipProtocolConfig(Builder builder) {
    this.maxGossipSent = builder.maxGossipSent;
    this.gossipTime = builder.gossipTime;
    this.maxMembersToSelect = builder.maxMembersToSelect;
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

  public int getMaxMembersToSelect() {
    return maxMembersToSelect;
  }

  @Override
  public String toString() {
    return "GossipProtocolConfig{maxGossipSent=" + maxGossipSent
        + ", gossipTime=" + gossipTime
        + ", maxMembersToSelect=" + maxMembersToSelect
        + '}';
  }

  public static final class Builder {

    private int maxGossipSent = DEFAULT_MAX_GOSSIP_SENT;
    private int gossipTime = DEFAULT_GOSSIP_TIME;
    private int maxMembersToSelect = DEFAULT_MAX_MEMBERS_TO_SELECT;

    private Builder() {}

    public Builder maxGossipSent(int maxGossipSent) {
      this.maxGossipSent = maxGossipSent;
      return this;
    }

    public Builder gossipTime(int gossipTime) {
      this.gossipTime = gossipTime;
      return this;
    }

    public Builder maxMembersToSelect(int maxMembersToSelect) {
      this.maxMembersToSelect = maxMembersToSelect;
      return this;
    }

    public GossipProtocolConfig build() {
      return new GossipProtocolConfig(this);
    }
  }
}
