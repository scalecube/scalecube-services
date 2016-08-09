package io.scalecube.transport;

import javax.annotation.concurrent.Immutable;

/**
 * Encapsulate transport settings.
 * 
 * @author Anton Kharenko
 */
@Immutable
public final class TransportConfig {

  public static final TransportConfig DEFAULT = builder().build();

  public static final int DEFAULT_CONNECT_TIMEOUT = 3000;
  public static final String DEFAULT_LOG_LEVEL = "OFF";
  public static final boolean DEFAULT_USE_NETWORK_EMULATOR = false;
  public static final boolean DEFAULT_ENABLE_EPOLL = true;
  public static final int DEFAULT_BOSS_THREADS = 2;
  public static final int DEFAULT_WORKER_THREADS = 0;

  private final int connectTimeout;
  private final String logLevel;
  private final boolean useNetworkEmulator;
  private final boolean enableEpoll;
  private final int bossThreads;
  private final int workerThreads;

  private TransportConfig(Builder builder) {
    this.connectTimeout = builder.connectTimeout;
    this.logLevel = builder.logLevel;
    this.useNetworkEmulator = builder.useNetworkEmulator;
    this.enableEpoll = builder.enableEpoll;
    this.bossThreads = builder.bossThreads;
    this.workerThreads = builder.workerThreads;
  }

  public static Builder builder() {
    return new Builder();
  }

  public int getConnectTimeout() {
    return connectTimeout;
  }

  public String getLogLevel() {
    return logLevel;
  }

  public boolean isUseNetworkEmulator() {
    return useNetworkEmulator;
  }

  public boolean isEnableEpoll() {
    return enableEpoll;
  }

  public int getBossThreads() {
    return bossThreads;
  }

  public int getWorkerThreads() {
    return workerThreads;
  }

  @Override
  public String toString() {
    return "TransportConfig{connectTimeout=" + connectTimeout
        + ", logLevel='" + logLevel + '\''
        + ", useNetworkEmulator=" + useNetworkEmulator
        + ", enableEpoll=" + enableEpoll
        + ", bossThreads=" + bossThreads
        + ", workerThreads=" + workerThreads
        + '}';
  }

  public static final class Builder {

    private String logLevel = DEFAULT_LOG_LEVEL;
    private boolean useNetworkEmulator = DEFAULT_USE_NETWORK_EMULATOR;
    private int connectTimeout = DEFAULT_CONNECT_TIMEOUT;
    private boolean enableEpoll = DEFAULT_ENABLE_EPOLL;
    private int bossThreads = DEFAULT_BOSS_THREADS;
    private int workerThreads = DEFAULT_WORKER_THREADS;

    private Builder() {}

    public Builder connectTimeout(int connectTimeout) {
      this.connectTimeout = connectTimeout;
      return this;
    }

    public Builder logLevel(String logLevel) {
      this.logLevel = logLevel;
      return this;
    }

    public Builder useNetworkEmulator(boolean useNetworkEmulator) {
      this.useNetworkEmulator = useNetworkEmulator;
      return this;
    }

    public Builder enableEpoll(boolean enableEpoll) {
      this.enableEpoll = enableEpoll;
      return this;
    }

    public Builder bossThreads(int bossThreads) {
      this.bossThreads = bossThreads;
      return this;
    }

    public Builder workerThreads(int workerThreads) {
      this.workerThreads = workerThreads;
      return this;
    }

    public TransportConfig build() {
      return new TransportConfig(this);
    }
  }
}
