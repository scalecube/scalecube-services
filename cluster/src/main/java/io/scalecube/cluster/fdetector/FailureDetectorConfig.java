package io.scalecube.cluster.fdetector;

import com.google.common.base.Preconditions;

public final class FailureDetectorConfig {

  public static final int DEFAULT_PING_INTERVAL = 1000;
  public static final int DEFAULT_PING_TIMEOUT = 500;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  private final int pingInterval;
  private final int pingTimeout;
  private final int pingReqMembers;

  private FailureDetectorConfig(Builder builder) {
    this.pingInterval = builder.pingInterval;
    this.pingTimeout = builder.pingTimeout;
    this.pingReqMembers = builder.pingReqMembers;
  }

  public static FailureDetectorConfig defaultConfig() {
    return builder().build();
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getPingInterval() {
    return pingInterval;
  }

  public int getPingTimeout() {
    return pingTimeout;
  }

  public int getPingReqMembers() {
    return pingReqMembers;
  }

  @Override
  public String toString() {
    return "FailureDetectorConfig{pingInterval=" + pingInterval
        + ", pingTimeout=" + pingTimeout
        + ", pingReqMembers=" + pingReqMembers
        + '}';
  }

  public static final class Builder {

    private int pingInterval = DEFAULT_PING_INTERVAL;
    private int pingTimeout = DEFAULT_PING_TIMEOUT;
    private int pingReqMembers = DEFAULT_PING_REQ_MEMBERS;

    private Builder() {}

    public Builder pingInterval(int pingInterval) {
      this.pingInterval = pingInterval;
      return this;
    }

    public Builder pingTimeout(int pingTimeout) {
      this.pingTimeout = pingTimeout;
      return this;
    }

    public Builder pingReqMembers(int pingReqMembers) {
      this.pingReqMembers = pingReqMembers;
      return this;
    }

    public FailureDetectorConfig build() {
      Preconditions.checkState(pingTimeout < pingInterval, "Ping timeout can't be bigger than ping interval");
      return new FailureDetectorConfig(this);
    }
  }
}
