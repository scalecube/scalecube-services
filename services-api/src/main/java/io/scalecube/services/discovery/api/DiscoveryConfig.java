package io.scalecube.services.discovery.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;

import java.util.HashMap;
import java.util.Map;

public class DiscoveryConfig {

  private Integer port;
  private Address[] seeds;
  private ServiceRegistry serviceRegistry;
  private Map<String, String> tags;
  private ServiceEndpoint endpoint;

  private DiscoveryConfig(Builder builder) {
    this.seeds = builder.seeds;
    this.serviceRegistry = builder.serviceRegistry;
    this.port = builder.port;
    this.tags = builder.tags;
    this.endpoint = builder.endpoint;
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

  public Map<String, String> tags(){
    return this.tags;
  }

  public ServiceEndpoint endpoint() {
    return this.endpoint;
  }
  public static Builder builder() {
    return new Builder();
  }

  public static class Builder {

    private Address[] seeds;
    private Integer port;
    private ServiceRegistry serviceRegistry;
    private Map<String, String> tags = new HashMap<>();
    private ServiceEndpoint endpoint;
    
    public Builder seeds(Address[] seeds) {
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

    public DiscoveryConfig build() {
      return new DiscoveryConfig(this);
    }

    public Builder tags(Map<String, String> tags) {
      this.tags = tags;
      return this;
    }

    public Builder endpoint(ServiceEndpoint endpoint) {
      this.endpoint = endpoint;
      return this;
    }

    public void aliasName(String aliasName) {
      tags.put("aliasName", aliasName);
    }
  }
}
