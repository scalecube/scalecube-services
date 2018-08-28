package io.scalecube.services.gateway;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.concurrent.Executor;

/** Represents gateway configuration. */
public final class GatewayConfig {

  private final String name;
  private final Class<? extends Gateway> gatewayClass;
  private final Map<String, String> options;
  private final int port;
  private final Executor workerThreadPool;

  private GatewayConfig(Builder builder) {
    name = builder.name;
    gatewayClass = builder.gatewayClass;
    port = builder.port;
    options = new HashMap<>(builder.options);
    workerThreadPool = builder.workerThreadPool;
  }

  /**
   * Gateway config identifier.
   *
   * @return config string identifier
   */
  public String name() {
    return name;
  }

  /**
   * Gateway class.
   *
   * @return gateway class
   */
  public Class<? extends Gateway> gatewayClass() {
    return gatewayClass;
  }

  /**
   * Gateway port.
   *
   * @return port number
   */
  public int port() {
    return port;
  }

  /**
   * Gateway worker thread pool.
   *
   * @return executor instance
   */
  public Executor workerThreadPool() {
    return workerThreadPool;
  }

  /**
   * Returns value of configuration property for given key.
   *
   * @param key configuration property name
   * @return property value
   */
  public Optional<String> get(String key) {
    return Optional.ofNullable(options.get(key));
  }

  public static Builder from(Builder other) {
    return new Builder(other);
  }

  public static Builder from(GatewayConfig config) {
    return new Builder(config);
  }

  public static Builder builder(String name, Class<? extends Gateway> gatewayClass) {
    return new Builder(name, gatewayClass);
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("GatewayConfig{");
    sb.append("name='").append(name).append("'");
    sb.append(", gatewayClass=").append(gatewayClass.getName());
    sb.append(", options=").append(options);
    sb.append(", port=").append(port);
    sb.append(", workerThreadPool=").append(workerThreadPool);
    sb.append('}');
    return sb.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (this == o) {
      return true;
    }
    if (o == null || getClass() != o.getClass()) {
      return false;
    }
    GatewayConfig that = (GatewayConfig) o;
    return Objects.equals(name, that.name) && Objects.equals(gatewayClass, that.gatewayClass);
  }

  @Override
  public int hashCode() {
    return Objects.hash(name, gatewayClass);
  }

  public static class Builder {

    private final String name;
    private final Class<? extends Gateway> gatewayClass;
    private Map<String, String> options = new HashMap<>();
    private int port = 0;
    private Executor workerThreadPool;

    private Builder(String name, Class<? extends Gateway> gatewayClass) {
      this.name = name;
      this.gatewayClass = gatewayClass;
    }

    private Builder(Builder other) {
      this.name = other.name;
      this.gatewayClass = other.gatewayClass;
      this.options = new HashMap<>(other.options);
      this.port = other.port;
      this.workerThreadPool = other.workerThreadPool;
    }

    private Builder(GatewayConfig config) {
      this.name = config.name;
      this.gatewayClass = config.gatewayClass;
      this.options = new HashMap<>(config.options);
      this.port = config.port;
      this.workerThreadPool = config.workerThreadPool;
    }

    public Builder port(int port) {
      this.port = port;
      return this;
    }

    public Builder workerThreadPool(Executor workerThreadPool) {
      this.workerThreadPool = workerThreadPool;
      return this;
    }

    public Builder addOption(String key, String value) {
      options.put(key, value);
      return this;
    }

    public Builder addOptions(Map<String, String> options) {
      this.options.putAll(options);
      return this;
    }

    public GatewayConfig build() {
      return new GatewayConfig(this);
    }
  }
}
