package io.scalecube.cluster.fdetector;

public final class FailureDetectorConfig {

  public static final FailureDetectorConfig DEFAULT = builder().build();

  public static final int DEFAULT_PING_TIME = 2000;
  public static final int DEFAULT_PING_TIMEOUT = 1000;
  public static final int DEFAULT_MAX_MEMBERS_TO_SELECT = 3;

  private final int pingTime;
  private final int pingTimeout;
  private final int maxMembersToSelect;

  private FailureDetectorConfig(Builder builder) {
    this.pingTime = builder.pingTime;
    this.pingTimeout = builder.pingTimeout;
    this.maxMembersToSelect = builder.maxMembersToSelect;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getPingTime() {
    return pingTime;
  }

  public int getPingTimeout() {
    return pingTimeout;
  }

  public int getMaxMembersToSelect() {
    return maxMembersToSelect;
  }

  @Override
  public String toString() {
    return "FailureDetectorConfig{pingTime=" + pingTime
        + ", pingTimeout=" + pingTimeout
        + ", maxMembersToSelect=" + maxMembersToSelect
        + '}';
  }

  public static final class Builder {

    private int pingTime = DEFAULT_PING_TIME;
    private int pingTimeout = DEFAULT_PING_TIMEOUT;
    private int maxMembersToSelect = DEFAULT_MAX_MEMBERS_TO_SELECT;

    private Builder() {}

    public Builder pingTime(int pingTime) {
      this.pingTime = pingTime;
      return this;
    }

    public Builder pingTimeout(int pingTimeout) {
      this.pingTimeout = pingTimeout;
      return this;
    }

    public Builder maxMembersToSelect(int maxMembersToSelect) {
      this.maxMembersToSelect = maxMembersToSelect;
      return this;
    }

    public FailureDetectorConfig build() {
      return new FailureDetectorConfig(this);
    }
  }
}
