package io.scalecube.transport;

import javax.annotation.concurrent.Immutable;

/**
 * Encapsulate transport settings.
 * 
 * @author Anton Kharenko
 */
@Immutable
public final class TransportSettings {

  public static final TransportSettings DEFAULT = builder().build();
  public static final TransportSettings DEFAULT_WITH_NETWORK_EMULATOR = builder().useNetworkEmulator(true).build();

  public static final int DEFAULT_CONNECT_TIMEOUT = 3000;
  public static final int DEFAULT_HANDSHAKE_TIMEOUT = 1000;
  public static final int DEFAULT_SEND_HIGH_WATER_MARK = 1000;
  public static final String DEFAULT_LOG_LEVEL = "OFF";
  public static final boolean DEFAULT_USE_NETWORK_EMULATOR = false;

  private final int connectTimeout;
  private final int handshakeTimeout;
  private final int sendHighWaterMark;
  private final String logLevel;
  private final boolean useNetworkEmulator;

  private TransportSettings(Builder builder) {
    this.connectTimeout = builder.connectTimeout;
    this.handshakeTimeout = builder.handshakeTimeout;
    this.sendHighWaterMark = builder.sendHighWaterMark;
    this.logLevel = builder.logLevel;
    this.useNetworkEmulator = builder.useNetworkEmulator;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public int getHandshakeTimeout() {
    return handshakeTimeout;
  }

  public int getSendHighWaterMark() {
    return sendHighWaterMark;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public boolean isUseNetworkEmulator() {
    return useNetworkEmulator;
  }

  @Override
  public String toString() {
    return "TransportSettings{"
        + "connectTimeout=" + connectTimeout
        + ", handshakeTimeout=" + handshakeTimeout
        + ", sendHighWaterMark=" + sendHighWaterMark
        + ", logLevel='" + logLevel + '\''
        + ", useNetworkEmulator=" + useNetworkEmulator
        + '}';
  }

  public static final class Builder {

    private String logLevel = DEFAULT_LOG_LEVEL;
    private boolean useNetworkEmulator = DEFAULT_USE_NETWORK_EMULATOR;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private int handshakeTimeout = DEFAULT_HANDSHAKE_TIMEOUT;
    private int sendHighWaterMark = DEFAULT_SEND_HIGH_WATER_MARK;

    private Builder() {}

    public Builder connectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder handshakeTimeout(int handshakeTimeout) {
      this.handshakeTimeout = handshakeTimeout;
      return this;
    }

    public Builder sendHighWaterMark(int sendHighWaterMark) {
      this.sendHighWaterMark = sendHighWaterMark;
      return this;
    }

    public Builder setLogLevel(String logLevel) {
      this.logLevel = logLevel;
      return this;
    }

    public Builder useNetworkEmulator(boolean useNetworkEmulator) {
      this.useNetworkEmulator = useNetworkEmulator;
      return this;
    }

    public TransportSettings build() {
      return new TransportSettings(this);
    }
  }
}
