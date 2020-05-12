package io.scalecube.services;

import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;

public class ServiceInfo {

  private final Object serviceInstance;
  private final Map<String, String> tags;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final PrincipalMapper<Object> principalMapper;

  private ServiceInfo(Builder builder) {
    this.serviceInstance = builder.serviceInstance;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;
    this.principalMapper = builder.principalMapper;
  }

  public static Builder from(ServiceInfo serviceInfo) {
    return new Builder(serviceInfo);
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

  public ServiceMessageDataDecoder dataDecoder() {
    return dataDecoder;
  }

  public PrincipalMapper<Object> principalMapper() {
    return principalMapper;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceInfo.class.getSimpleName() + "[", "]")
        .add("serviceInstance=" + serviceInstance)
        .add("tags=" + tags)
        .add("errorMapper=" + errorMapper)
        .add("dataDecoder=" + dataDecoder)
        .add("principalMapper=" + principalMapper)
        .toString();
  }

  public static class Builder {

    private Object serviceInstance;
    private Map<String, String> tags = new HashMap<>();
    private ServiceProviderErrorMapper errorMapper;
    private ServiceMessageDataDecoder dataDecoder;
    private PrincipalMapper<Object> principalMapper;

    private Builder(ServiceInfo serviceInfo) {
      this.serviceInstance = serviceInfo.serviceInstance;
      this.tags.putAll(new HashMap<>(serviceInfo.tags));
      this.errorMapper = serviceInfo.errorMapper;
      this.dataDecoder = serviceInfo.dataDecoder;
      this.principalMapper = serviceInfo.principalMapper;
    }

    private Builder(Object serviceInstance) {
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

    Builder errorMapperIfAbsent(ServiceProviderErrorMapper errorMapper) {
      if (this.errorMapper == null) {
        this.errorMapper = errorMapper;
      }
      return this;
    }

    public Builder dataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = dataDecoder;
      return this;
    }

    Builder dataDecoderIfAbsent(ServiceMessageDataDecoder dataDecoder) {
      if (this.dataDecoder == null) {
        this.dataDecoder = dataDecoder;
      }
      return this;
    }

    @SuppressWarnings("unchecked")
    public <T> Builder principalMapper(PrincipalMapper<? extends T> principalMapper) {
      this.principalMapper = (PrincipalMapper<Object>) principalMapper;
      return this;
    }

    Builder principalMapperIfAbsent(PrincipalMapper<Object> principalMapper) {
      if (this.principalMapper == null) {
        this.principalMapper = principalMapper;
      }
      return this;
    }

    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
