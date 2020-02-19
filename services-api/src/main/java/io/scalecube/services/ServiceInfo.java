package io.scalecube.services;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.StringJoiner;
import java.util.function.Supplier;

@SuppressWarnings("rawtypes")
public class ServiceInfo {

  private final Class<?> serviceType;
  private final Supplier<Object> serviceInstanceSupplier;
  private final Map<String, String> tags;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final Authenticator authenticator;

  private ServiceInfo(Builder builder) {
    this.serviceType = builder.serviceType;
    this.serviceInstanceSupplier = builder.serviceInstanceSupplier;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;
    this.authenticator = builder.authenticator;
  }

  public static Builder from(ServiceInfo serviceInfo) {
    return new Builder(serviceInfo);
  }

  public static Builder fromServiceInstance(Object serviceInstance) {
    return new Builder(serviceInstance.getClass(), () -> serviceInstance);
  }

  public Supplier<Object> serviceInstanceSupplier() {
    return serviceInstanceSupplier;
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

  public Authenticator authenticator() {
    return authenticator;
  }

  public Class<?> type() {
    return serviceType;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceInfo.class.getSimpleName() + "[", "]")
        .add("serviceInstance=" + serviceInstanceSupplier)
        .add("tags=" + tags)
        .add("errorMapper=" + errorMapper)
        .add("dataDecoder=" + dataDecoder)
        .add("authenticator=" + authenticator)
        .toString();
  }

  @SuppressWarnings("rawtypes")
  public static class Builder {

    private final Class<?> serviceType;
    private final Supplier<Object> serviceInstanceSupplier;
    private Map<String, String> tags = new HashMap<>();
    private ServiceProviderErrorMapper errorMapper;
    private ServiceMessageDataDecoder dataDecoder;
    private Authenticator authenticator;

    private Builder(ServiceInfo serviceInfo) {
      this.serviceInstanceSupplier = serviceInfo.serviceInstanceSupplier;
      this.serviceType = serviceInfo.serviceType;
      this.tags.putAll(new HashMap<>(serviceInfo.tags));
      this.errorMapper = serviceInfo.errorMapper;
      this.dataDecoder = serviceInfo.dataDecoder;
      this.authenticator = serviceInfo.authenticator;
    }

    private Builder(Class<?> serviceType, Supplier<Object> serviceInstanceSupplier) {
      this.serviceType = serviceType;
      this.serviceInstanceSupplier = serviceInstanceSupplier;
    }

    public Builder tag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    public Builder dataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = dataDecoder;
      return this;
    }

    public Builder authenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
      return this;
    }

    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
