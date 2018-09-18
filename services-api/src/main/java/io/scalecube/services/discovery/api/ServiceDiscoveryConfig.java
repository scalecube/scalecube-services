package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;

public class ServiceDiscoveryConfig {

  private Integer port;
  private Address[] seeds;
  private ServiceRegistry serviceRegistry;
  private Map<String, String> tags;
  private ServiceEndpoint endpoint;
  private String memberHost;
  private Integer memberPort;

  private ServiceDiscoveryConfig(Builder builder) {
    this.seeds = builder.seeds;
    this.serviceRegistry = builder.serviceRegistry;
    this.port = builder.port;
    this.tags = new HashMap<>(builder.tags);
    this.endpoint = builder.endpoint;
    this.memberHost = builder.memberHost;
    this.memberPort = builder.memberPort;
  }

  public Integer port() {
    return port;
  }

  public Address[] seeds() {
    return seeds;
  }

  public ServiceRegistry serviceRegistry() {
    return serviceRegistry;
  }

  public Map<String, String> tags() {
    return this.tags;
  }

  public ServiceEndpoint endpoint() {
    return this.endpoint;
  }

  public String memberHost() {
    return memberHost;
  }

  public Integer memberPort() {
    return memberPort;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * Returns a new discovery config builder and apply to it the given discovery options.
   *
   * @param discoveryOptions discovery options
   * @return discovery config builder
   */
  public static Builder builder(Consumer<Builder> discoveryOptions) {
    Builder builder = new Builder();
    if (discoveryOptions != null) {
      discoveryOptions.accept(builder);
    }
    return builder;
  }

  public static class Builder {

    private Address[] seeds;
    private Integer port;
    private ServiceRegistry serviceRegistry;
    private Map<String, String> tags = Collections.emptyMap();
    private ServiceEndpoint endpoint;
    private String memberHost;
    private Integer memberPort;

    public Builder seeds(Address... seeds) {
      this.seeds = seeds;
      return this;
    }

    public Builder port(Integer port) {
      this.port = port;
      return this;
    }

    public Builder serviceRegistry(ServiceRegistry serviceRegistry) {
      this.serviceRegistry = serviceRegistry;
      return this;
    }

    public ServiceDiscoveryConfig build() {
      return new ServiceDiscoveryConfig(this);
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder endpoint(ServiceEndpoint endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public Builder memberHost(String memberHost) {
      this.memberHost = memberHost;
      return this;
    }

    public Builder memberPort(Integer memberPort) {
      this.memberPort = memberPort;
      return this;
    }
  }

  @Override
  public String toString() {
    final StringBuilder sb = new StringBuilder("ServiceDiscoveryConfig{");
    sb.append("port=").append(port);
    sb.append(", seeds=").append(Arrays.toString(seeds));
    sb.append(", serviceRegistry=").append(serviceRegistry);
    sb.append(", tags=").append(tags);
    sb.append(", endpoint=").append(endpoint);
    sb.append(", memberHost='").append(memberHost).append('\'');
    sb.append(", memberPort=").append(memberPort);
    sb.append('}');
    return sb.toString();
  }
}
