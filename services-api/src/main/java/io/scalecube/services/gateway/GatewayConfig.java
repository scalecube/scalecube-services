package io.scalecube.services.gateway;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents gateway configuration.
 */
public final class GatewayConfig {

  private final Class<? extends Gateway> gatewayClass;

  private final Map<String, String> options;

  private final int port;

  private GatewayConfig(Builder builder) {
    gatewayClass = builder.gatewayClass;
    port = builder.port;
    options = new HashMap<>(builder.options);
  }

  /**
   * Gateway class.
   *
   * @return gateway class.
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

  public static Builder builder(Class<? extends Gateway> gatewayClass) {
    return new Builder(gatewayClass);
  }

  @Override
  public String toString() {
    return "GatewayConfig{" +
        "gatewayClass=" + gatewayClass +
        "options=" + options +
        ", port=" + port +
        '}';
  }

  public static class Builder {

    private final Class<? extends Gateway> gatewayClass;

    private Map<String, String> options = new HashMap<>();

    private int port;

    private Builder(Builder other) {
      this.gatewayClass = other.gatewayClass;
      this.options = new HashMap<>(other.options);
      this.port = other.port;
    }

    private Builder(Class<? extends Gateway> gatewayClass) {
      this.gatewayClass = gatewayClass;
    }

    public Builder port(int port) {
      this.port = port;
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
