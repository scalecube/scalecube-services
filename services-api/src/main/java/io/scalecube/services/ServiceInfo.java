package io.scalecube.services;

import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.StringJoiner;

public class ServiceInfo {

  private final Object serviceInstance;
  private final Map<String, String> tags;
  private final ServiceProviderErrorMapper errorMapper;
  private final ServiceMessageDataDecoder dataDecoder;
  private final PrincipalMapper<Object, Object> principalMapper;

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

  public PrincipalMapper<Object, Object> principalMapper() {
    return principalMapper;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceInfo.class.getSimpleName() + "[", "]")
        .add("serviceInstance=" + serviceInstance)
        .add("tags(" + tags.size() + ")")
        .add("errorMapper=" + errorMapper)
        .add("dataDecoder=" + dataDecoder)
        .add("principalMapper=" + principalMapper)
        .toString();
  }

  public static class Builder {

    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();
    private ServiceProviderErrorMapper errorMapper;
    private ServiceMessageDataDecoder dataDecoder;
    private PrincipalMapper<Object, Object> principalMapper;

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

    /**
     * Setter for {@code tags}. Merges this {@code tags} with {@code Microservices.tags}. If keys
     * are clashing this {@code tags} shall override {@code Microservices.tags}.
     *
     * @param key tag key; not null
     * @param value tag value; not null
     * @return this builder
     */
    public Builder tag(String key, String value) {
      Objects.requireNonNull(key, "tag key");
      Objects.requireNonNull(value, "tag value");
      tags.put(key, value);
      return this;
    }

    /**
     * Setter for {@code errorMapper}. Overrides default {@code Microservices.errorMapper}.
     *
     * @param errorMapper error mapper; not null
     * @return this buidler
     */
    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
      return this;
    }

    /**
     * Setter for {@code dataDecoder}. Overrides default {@code Microservices.dataDecoder}.
     *
     * @param dataDecoder data decoder; not null
     * @return this builder
     */
    public Builder dataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
      return this;
    }

    /**
     * Setter for {@code principalMapper}. Overrides default {@code Microservices.principalMapper}.
     *
     * @param principalMapper principalMapper
     * @param <A> type of auth data returned by authenticator
     * @param <T> type of principal after mapping auth data to principal
     * @return this builder
     */
    @SuppressWarnings("unchecked")
    public <A, T> Builder principalMapper(
        PrincipalMapper<? extends A, ? extends T> principalMapper) {
      Objects.requireNonNull(principalMapper, "principalMapper");
      this.principalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
    }

    Builder errorMapperIfAbsent(ServiceProviderErrorMapper errorMapper) {
      if (this.errorMapper == null) {
        this.errorMapper = errorMapper;
      }
      return this;
    }

    Builder dataDecoderIfAbsent(ServiceMessageDataDecoder dataDecoder) {
      if (this.dataDecoder == null) {
        this.dataDecoder = dataDecoder;
      }
      return this;
    }

    Builder principalMapperIfAbsent(PrincipalMapper<Object, Object> principalMapper) {
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
