package io.scalecube.services;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ServicesConfig {

  private static final ServicesConfig EMPTY_SERVICES_CONFIG = new ServicesConfig();

  private List<ServiceConfig> servicesConfig = new ArrayList<>();

  public void setServiceTags(List<ServiceConfig> serviceTags) {
    this.servicesConfig = serviceTags;
  }

  ServicesConfig() {}

  ServicesConfig(List<ServiceConfig> unmodifiableList) {
    this.servicesConfig = unmodifiableList;
  }

  public List<ServiceConfig> services() {
    return this.servicesConfig;
  }

  public static Builder builder(Microservices.Builder builder) {
    return new Builder(builder);
  }

  public static class Builder {
    private List<ServiceConfig> servicesBuilder = new ArrayList<>();
    private Microservices.Builder microservicesBuilder;

    public static class ServiceConfig {

      private final Builder builder;

      private final Object service;

      private final Map<String, String> kv = new HashMap<String, String>();

      private final Set<ServiceDefinition> serviceDefinitions;

      public ServiceConfig(Builder builder, Object service) {
        this.service = service;
        this.serviceDefinitions = ServiceDefinition.from(service);
        this.builder = builder;
      }

      public ServiceConfig(Object service) {
        this.service = service;
        this.serviceDefinitions = ServiceDefinition.from(service);
        this.builder = null;
      }

      public ServiceConfig tag(String key, String value) {
        kv.put(key, value);
        return this;
      }

      public Builder add() {
        return builder.add(this);
      }

      public Set<ServiceDefinition> getDefinitions() {
        return this.serviceDefinitions;
      }

      public Map<String, String> getTags() {
        return Collections.unmodifiableMap(kv);
      }

      public Object getService() {
        return this.service;
      }

      public Set<String> serviceNames() {
        return serviceDefinitions.stream()
            .map(definition -> definition.serviceName())
            .collect(Collectors.toSet());
      }

      /**
       * return method names for a given service name.
       * 
       * @param serviceName to retrieve.
       * @return list of declared method names for a given service.
       */
      public Set<String> methods(String serviceName) {
        return serviceDefinitions.stream()
            .filter(def -> def.serviceName().equals(serviceName))
            .findFirst().get()
            .methods()
            .keySet();
      }
    }

    public Builder(Microservices.Builder builder) {
      this.microservicesBuilder = builder;
    }

    public ServiceConfig service(Object object) {
      return new ServiceConfig(this, object);
    }

    public Builder add(ServiceConfig serviceBuilder) {
      servicesBuilder.add(serviceBuilder);
      return this;
    }

    public Microservices.Builder build() {
      return microservicesBuilder.services(
          new ServicesConfig(Collections.unmodifiableList(servicesBuilder)));
    }

    public ServicesConfig create() {
      return new ServicesConfig(Collections.unmodifiableList(servicesBuilder));
    }

    public Builder services(Object[] objects) {
      for (Object o : objects) {
        this.add(new ServiceConfig(o));
      }
      return this;
    }
  }

  public static ServicesConfig empty() {
    return EMPTY_SERVICES_CONFIG;
  }
}
