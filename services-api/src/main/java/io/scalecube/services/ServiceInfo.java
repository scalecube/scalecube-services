package io.scalecube.services;

import io.scalecube.services.auth.Authenticator;
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
  private final Authenticator<Object> authenticator;
  private final PrincipalMapper<Object, Object> principalMapper;

  private ServiceInfo(Builder builder) {
    this.serviceInstance = builder.serviceInstance;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;
    this.authenticator = builder.authenticator;
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

  public Authenticator<Object> authenticator() {
    return authenticator;
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
        .add("authenticator=" + authenticator)
        .add("principalMapper=" + principalMapper)
        .toString();
  }

  public static class Builder {

    private final Object serviceInstance;
    private Map<String, String> tags = new HashMap<>();
    private ServiceProviderErrorMapper errorMapper;
    private ServiceMessageDataDecoder dataDecoder;
    private Authenticator<Object> authenticator;
    private PrincipalMapper<Object, Object> principalMapper;

    private Builder(ServiceInfo serviceInfo) {
      this.serviceInstance = serviceInfo.serviceInstance;
      this.tags.putAll(new HashMap<>(serviceInfo.tags));
      this.errorMapper = serviceInfo.errorMapper;
      this.dataDecoder = serviceInfo.dataDecoder;
      this.authenticator = serviceInfo.authenticator;
      this.principalMapper = serviceInfo.principalMapper;
    }

    private Builder(Object serviceInstance) {
      this.serviceInstance = serviceInstance;
    }

    /**
     * Add tag for service info.
     *
     * @param key tag name.
     * @param value tag value.
     * @return current builder's state.
     */
    public Builder tag(String key, String value) {
      tags.put(key, value);
      return this;
    }

    /**
     * Set up {@link ServiceProviderErrorMapper}.
     *
     * @param errorMapper error mapper.
     * @return current builder's state.
     */
    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = errorMapper;
      return this;
    }

    /**
     * Set up {@link ServiceMessageDataDecoder}.
     *
     * @param dataDecoder data decoder.
     * @return current builder's state.
     */
    public Builder dataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = dataDecoder;
      return this;
    }

    /**
     * Set up {@link Authenticator}.
     *
     * @param authenticator authenticator.
     * @return current builder's state.
     */
    @SuppressWarnings("unchecked")
    public <T> Builder authenticator(Authenticator<? extends T> authenticator) {
      this.authenticator = (Authenticator<Object>) authenticator;
      return this;
    }

    @SuppressWarnings("unchecked")
    public <A, T> Builder principalMapper(
        PrincipalMapper<? extends A, ? extends T> principalMapper) {
      this.principalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
    }

    /**
     * Set up {@link ServiceProviderErrorMapper} if it hasn't been set up before.
     *
     * @param errorMapper error mapper.
     * @return current builder's state.
     */
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

    /**
     * Set up {@link Authenticator} if it hasn't been set up before.
     *
     * @param authenticator authenticator.
     * @return current builder's state.
     */
    Builder authenticatorIfAbsent(Authenticator<Object> authenticator) {
      if (this.authenticator == null) {
        this.authenticator = authenticator;
      }
      return this;
    }

    Builder principalMapperIfAbsent(PrincipalMapper<Object, Object> principalMapper) {
      if (this.principalMapper == null) {
        this.principalMapper = principalMapper;
      }
      return this;
    }

    /**
     * Build service info.
     *
     * @return service info.
     */
    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
