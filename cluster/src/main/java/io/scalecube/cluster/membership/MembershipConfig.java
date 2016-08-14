package io.scalecube.cluster.membership;

public final class MembershipConfig {

  public static final MembershipConfig DEFAULT = builder().build();

  public static final int DEFAULT_SYNC_TIME = 30 * 1000;
  public static final int DEFAULT_SYNC_TIMEOUT = 3 * 1000;
  public static final int DEFAULT_MAX_SUSPECT_TIME = 60 * 1000;
  public static final int DEFAULT_MAX_SHUTDOWN_TIME = 60 * 1000;
  public static final String DEFAULT_SYNC_GROUP = "default";

  private final int syncTime;
  private final int syncTimeout;
  private final int maxSuspectTime;
  private final int maxShutdownTime;
  private final String syncGroup;

  private MembershipConfig(Builder builder) {
    this.syncTime = builder.syncTime;
    this.syncTimeout = builder.syncTimeout;
    this.maxSuspectTime = builder.maxSuspectTime;
    this.maxShutdownTime = builder.maxShutdownTime;
    this.syncGroup = builder.syncGroup;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getSyncTime() {
    return syncTime;
  }

  public int getSyncTimeout() {
    return syncTimeout;
  }

  public int getMaxSuspectTime() {
    return maxSuspectTime;
  }

  public int getMaxShutdownTime() {
    return maxShutdownTime;
  }

  public String getSyncGroup() {
    return syncGroup;
  }

  @Override
  public String toString() {
    return "MembershipConfigF{syncTime=" + syncTime
        + ", syncTimeout=" + syncTimeout
        + ", maxSuspectTime=" + maxSuspectTime
        + ", maxShutdownTime=" + maxShutdownTime
        + ", syncGroup='" + syncGroup + '\''
        + '}';
  }

  public static final class Builder {

    private int syncTime = DEFAULT_SYNC_TIME;
    private int syncTimeout = DEFAULT_SYNC_TIMEOUT;
    private int maxSuspectTime = DEFAULT_MAX_SUSPECT_TIME;
    private int maxShutdownTime = DEFAULT_MAX_SHUTDOWN_TIME;
    private String syncGroup = DEFAULT_SYNC_GROUP;

    private Builder() {}

    public Builder syncTime(int syncTime) {
      this.syncTime = syncTime;
      return this;
    }

    public Builder syncTimeout(int syncTimeout) {
      this.syncTimeout = syncTimeout;
      return this;
    }

    public Builder maxSuspectTime(int maxSuspectTime) {
      this.maxSuspectTime = maxSuspectTime;
      return this;
    }

    public Builder maxShutdownTime(int maxShutdownTime) {
      this.maxShutdownTime = maxShutdownTime;
      return this;
    }

    public Builder syncGroup(String syncGroup) {
      this.syncGroup = syncGroup;
      return this;
    }

    public MembershipConfig build() {
      return new MembershipConfig(this);
    }
  }
}
