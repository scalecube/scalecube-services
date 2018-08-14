package io.scalecube.services.gateway;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

/**
 * Represents gateway configuration.
 */
public final class GatewayConfig {

  private final Map<String, String> options;

  private Integer port;

  private GatewayConfig(Builder builder) {
    port = builder.port;
    options = Collections.unmodifiableMap(builder.options);
  }

  /**
   * Gateway port.
   * 
   * @return port number
   */
  public Optional<Integer> port() {
    return Optional.ofNullable(port);
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

  public static Builder builder() {
    return new Builder();
  }

  @Override
  public String toString() {
    return "GatewayConfig{" +
        "options=" + options +
        ", port=" + port +
        '}';
  }

  public static class Builder {

    private final Map<String, String> options = new HashMap<>();

    private Integer port;

    private Builder() {}

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
