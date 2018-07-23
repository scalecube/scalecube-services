package io.scalecube.services;

import java.util.Collections;
import java.util.HashMap;
import java.util.Map;

public class ServiceInfo {
  private final Object serviceInstance;
  private final Map<String, String> tags;

  public ServiceInfo(Builder builder) {
    this.serviceInstance = builder.serviceInstance;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
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

  public static class Builder {
    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();

    public Builder(Object serviceInstance) {
      this.serviceInstance = serviceInstance;
    }

    public Builder tag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
