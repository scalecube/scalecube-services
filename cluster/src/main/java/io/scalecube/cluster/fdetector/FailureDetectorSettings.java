package io.scalecube.cluster.fdetector;

public final class FailureDetectorSettings {

  public static final FailureDetectorSettings DEFAULT = builder().build();

  public static final int DEFAULT_PING_TIME = 2000;
  public static final int DEFAULT_PING_TIMEOUT = 1000;
  public static final int DEFAULT_MAX_ENDPOINTS_TO_SELECT = 3;

  private final int pingTime;
  private final int pingTimeout;
  private final int maxEndpointsToSelect;

  private FailureDetectorSettings(Builder builder) {
    this.pingTime = builder.pingTime;
    this.pingTimeout = builder.pingTimeout;
    this.maxEndpointsToSelect = builder.maxEndpointsToSelect;
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

  public int getMaxEndpointsToSelect() {
    return maxEndpointsToSelect;
  }

  @Override
  public String toString() {
    return "FailureDetectorSettings{pingTime=" + pingTime
        + ", pingTimeout=" + pingTimeout
        + ", maxEndpointsToSelect=" + maxEndpointsToSelect
        + '}';
  }

  public static final class Builder {

    private int pingTime = DEFAULT_PING_TIME;
    private int pingTimeout = DEFAULT_PING_TIMEOUT;
    private int maxEndpointsToSelect = DEFAULT_MAX_ENDPOINTS_TO_SELECT;

    private Builder() {}

    public Builder pingTime(int pingTime) {
      this.pingTime = pingTime;
      return this;
    }

    public Builder pingTimeout(int pingTimeout) {
      this.pingTimeout = pingTimeout;
      return this;
    }

    public Builder maxEndpointsToSelect(int maxEndpointsToSelect) {
      this.maxEndpointsToSelect = maxEndpointsToSelect;
      return this;
    }

    public FailureDetectorSettings build() {
      return new FailureDetectorSettings(this);
    }
  }
}
