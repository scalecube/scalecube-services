package io.scalecube.services;

import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ServiceInfo {
  private final Object serviceInstance;
  private final Map<String, String> tags;
  private final ServiceProviderErrorMapper errorMapper;

  private ServiceInfo(Builder builder) {
    this.serviceInstance = builder.serviceInstance;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.errorMapper = builder.errorMapper;
  }

  public static Builder fromServiceInstance(Object serviceInstance) {
    return new Builder(serviceInstance);
  }

  public Object serviceInstance() {
    return serviceInstance;
  }

  public Map<String, String> tags() {
    return tags;
  }

  public ServiceProviderErrorMapper errorMapper() {
    return errorMapper;
  }

  public static class Builder {
    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();
    private ServiceProviderErrorMapper errorMapper;

    public Builder(Object serviceInstance) {
      this.serviceInstance = serviceInstance;
    }

    public Builder tag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
