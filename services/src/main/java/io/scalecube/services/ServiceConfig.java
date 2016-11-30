package io.scalecube.services;

import io.scalecube.services.ServiceConfig.Builder.ServiceContext;
import io.scalecube.transport.Address;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;

public class ServiceConfig {

  private List<ServiceContext> serviceTags = new ArrayList<>();

  public List<ServiceContext> getServiceTags() {
    return serviceTags;
  }

  public void setServiceTags(List<ServiceContext> serviceTags) {
    this.serviceTags = serviceTags;
  }

  ServiceConfig() {}

  ServiceConfig(List<ServiceContext> unmodifiableList) {
    this.serviceTags = unmodifiableList;
  }

  public List<ServiceContext> services() {
    return this.serviceTags;
  }

  public static Builder builder() {
    return new Builder();
  }

  static class Builder {
    List<ServiceContext> services = new ArrayList();

    static class ServiceContext {

      private final Object service;

      private final Map<String, String> kv = new HashMap<String, String>();
      private final Builder builder;

      public ServiceContext(Builder builder, Object service) {
        this.service = service;
        this.builder = builder;
      }

      public ServiceContext(Object obj) {
        this.service = obj;
        this.builder = null;
      }

      public ServiceContext tag(String key, String value) {
        kv.put(key, value);
        return this;
      }

      public Builder add() {
        return builder.add(this);
      }

      public Tag[] getTags() {

        List<Tag> tagList = kv.entrySet().stream()
            .map(entry -> toTag(entry))
            .collect(Collectors.toList());

        return tagList.toArray(new Tag[tagList.size()]);
      }

      private Tag toTag(Entry<String, String> entry) {
        return new Tag(entry.getKey(), entry.getValue());
      }

      public Object getService() {
        return this.service;
      }

    }

    public ServiceContext service(Object object) {
      return new ServiceContext(this, object);
    }

    Builder add(ServiceContext serviceBuilder) {
      services.add(serviceBuilder);
      return this;
    }

    public ServiceConfig build() {
      return new ServiceConfig(Collections.unmodifiableList(services));
    }

    public Builder services(Object[] objects) {
      for (Object o : objects) {
        this.add(new ServiceContext(o));
      }
      return this;
    }
  }

  public static ServiceConfig from(Address address,Object[] services) {
    return ServiceConfig.builder().services(services).build();
  }
}
