package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.BiConsumer;
import java.util.function.UnaryOperator;

public final class ServiceInfo {

  private final Object serviceInstance;
  private final Map<String, String> tags;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final Authenticator authenticator;
  private final UnaryOperator<ServiceMessage> requestMapper;
  private final UnaryOperator<ServiceMessage> responseMapper;
  private final BiConsumer<ServiceMessage, Throwable> errorHandler;

  private ServiceInfo(Builder builder) {
    this.serviceInstance = builder.serviceInstance;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;
    this.authenticator = builder.authenticator;
    this.requestMapper = builder.requestMapper;
    this.responseMapper = builder.responseMapper;
    this.errorHandler = builder.errorHandler;
  }

  public static Builder from(ServiceInfo other) {
    return new Builder(other);
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

  public Authenticator authenticator() {
    return authenticator;
  }

  public UnaryOperator<ServiceMessage> requestMapper() {
    return requestMapper;
  }

  public UnaryOperator<ServiceMessage> responseMapper() {
    return responseMapper;
  }

  public BiConsumer<ServiceMessage, Throwable> errorHandler() {
    return errorHandler;
  }

  public static class Builder {

    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();

    private ServiceProviderErrorMapper errorMapper;
    private ServiceMessageDataDecoder dataDecoder;
    private Authenticator authenticator;
    private UnaryOperator<ServiceMessage> requestMapper;
    private UnaryOperator<ServiceMessage> responseMapper;
    private BiConsumer<ServiceMessage, Throwable> errorHandler;

    /**
     * Copy builder.
     *
     * @param other other instace
     */
    public Builder(ServiceInfo other) {
      this.serviceInstance = other.serviceInstance;
      this.tags.putAll(other.tags);
      this.errorMapper = other.errorMapper;
      this.dataDecoder = other.dataDecoder;
      this.authenticator = other.authenticator;
      this.requestMapper = other.requestMapper;
      this.responseMapper = other.responseMapper;
      this.errorHandler = other.errorHandler;
    }

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

    public Builder dataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = dataDecoder;
      return this;
    }

    public Builder authenticator(Authenticator authenticator) {
      this.authenticator = authenticator;
      return this;
    }

    public Builder requestMapper(UnaryOperator<ServiceMessage> requestMapper) {
      this.requestMapper = requestMapper;
      return this;
    }

    public Builder responseMapper(UnaryOperator<ServiceMessage> responseMapper) {
      this.responseMapper = responseMapper;
      return this;
    }

    public Builder errorHandler(BiConsumer<ServiceMessage, Throwable> errorHandler) {
      this.errorHandler = errorHandler;
      return this;
    }

    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
