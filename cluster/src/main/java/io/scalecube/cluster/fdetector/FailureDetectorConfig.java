package io.scalecube.cluster.fdetector;

public final class FailureDetectorConfig {

  public static final int DEFAULT_PING_TIME = 2000;
  public static final int DEFAULT_PING_TIMEOUT = 1000;
  public static final int DEFAULT_PING_REQ_MEMBERS = 3;

  private final int pingTime;
  private final int pingTimeout;
  private final int pingReqMembers;

  private FailureDetectorConfig(Builder builder) {
    this.pingTime = builder.pingTime;
    this.pingTimeout = builder.pingTimeout;
    this.pingReqMembers = builder.pingReqMembers;
  }

  public static FailureDetectorConfig defaultConfig() {
    return builder().build();
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

  public int getPingReqMembers() {
    return pingReqMembers;
  }

  @Override
  public String toString() {
    return "FailureDetectorConfig{pingTime=" + pingTime
        + ", pingTimeout=" + pingTimeout
        + ", pingReqMembers=" + pingReqMembers
        + '}';
  }

  public static final class Builder {

    private int pingTime = DEFAULT_PING_TIME;
    private int pingTimeout = DEFAULT_PING_TIMEOUT;
    private int pingReqMembers = DEFAULT_PING_REQ_MEMBERS;

    private Builder() {}

    public Builder pingTime(int pingTime) {
      this.pingTime = pingTime;
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
      return new FailureDetectorConfig(this);
    }
  }
}
