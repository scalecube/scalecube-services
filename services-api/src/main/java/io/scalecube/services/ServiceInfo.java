package io.scalecube.services;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
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
  private final Logger logger;
  private final Level level;

  private ServiceInfo(Builder builder) {
    this.serviceInstance = builder.serviceInstance;
    this.tags = Collections.unmodifiableMap(new HashMap<>(builder.tags));
    this.errorMapper = builder.errorMapper;
    this.dataDecoder = builder.dataDecoder;
    this.authenticator = builder.authenticator;
    this.principalMapper = builder.principalMapper;
    this.logger = builder.logger;
    this.level = builder.level;
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

  public Logger logger() {
    return logger;
  }

  public Level level() {
    return level;
  }

  @Override
  public String toString() {
    return new StringJoiner(", ", ServiceInfo.class.getSimpleName() + "[", "]")
        .add("serviceInstance=" + serviceInstance)
        .add("tags=" + tags)
        .add("errorMapper=" + errorMapper)
        .add("dataDecoder=" + dataDecoder)
        .add("authenticator=" + authenticator)
        .add("principalMapper=" + principalMapper)
        .add("logger=" + logger)
        .add("level=" + level)
        .toString();
  }

  public static class Builder {

    private final Object serviceInstance;
    private final Map<String, String> tags = new HashMap<>();
    private ServiceProviderErrorMapper errorMapper;
    private ServiceMessageDataDecoder dataDecoder;
    private Authenticator<Object> authenticator;
    private PrincipalMapper<Object, Object> principalMapper;
    private Logger logger;
    private Level level;

    private Builder(ServiceInfo serviceInfo) {
      this.serviceInstance = serviceInfo.serviceInstance;
      this.tags.putAll(new HashMap<>(serviceInfo.tags));
      this.errorMapper = serviceInfo.errorMapper;
      this.dataDecoder = serviceInfo.dataDecoder;
      this.authenticator = serviceInfo.authenticator;
      this.principalMapper = serviceInfo.principalMapper;
      this.logger = serviceInfo.logger;
      this.level = serviceInfo.level;
    }

    private Builder(Object serviceInstance) {
      this.serviceInstance = serviceInstance;
    }

    /**
     * Setter for {@code tags}. Merges this {@code tags} with {@code Microservices.tags}. If keys
     * are clashing this {@code tags} shall override {@code Microservices.tags}.
     *
     * @param key tag key
     * @param value tag value
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
     * @param errorMapper error mapper
     * @return this buidler
     */
    public Builder errorMapper(ServiceProviderErrorMapper errorMapper) {
      this.errorMapper = Objects.requireNonNull(errorMapper, "errorMapper");
      return this;
    }

    /**
     * Setter for {@code logger}. Overrides default {@code Microservices.logger}.
     *
     * @param name logger name (optional)
     * @param level logger level (optional)
     * @return this buidler
     */
    public Builder logger(String name, Level level) {
      this.logger = name != null ? System.getLogger(name) : null;
      this.level = level;
      return this;
    }

    /**
     * Setter for {@code dataDecoder}. Overrides default {@code Microservices.dataDecoder}.
     *
     * @param dataDecoder data decoder
     * @return this builder
     */
    public Builder dataDecoder(ServiceMessageDataDecoder dataDecoder) {
      this.dataDecoder = Objects.requireNonNull(dataDecoder, "dataDecoder");
      return this;
    }

    /**
     * Setter for {@code authenticator}. Overrides default {@code Microservices.authenticator}.
     *
     * @param authenticator authenticator (optional)
     * @param <T> type of auth data returned by authenticator
     * @return this builder
     */
    public <T> Builder authenticator(Authenticator<? extends T> authenticator) {
      //noinspection unchecked
      this.authenticator = (Authenticator<Object>) authenticator;
      return this;
    }

    /**
     * Setter for {@code principalMapper}. Overrides default {@code Microservices.principalMapper}.
     *
     * @param principalMapper principalMapper (optional)
     * @param <T> auth data type
     * @param <R> principal type
     * @return this builder
     */
    public <T, R> Builder principalMapper(PrincipalMapper<? super T, ? extends R> principalMapper) {
      //noinspection unchecked
      this.principalMapper = (PrincipalMapper<Object, Object>) principalMapper;
      return this;
    }

    Builder errorMapperIfAbsent(ServiceProviderErrorMapper errorMapper) {
      if (this.errorMapper == null) {
        return errorMapper(errorMapper);
      }
      return this;
    }

    Builder dataDecoderIfAbsent(ServiceMessageDataDecoder dataDecoder) {
      if (this.dataDecoder == null) {
        return dataDecoder(dataDecoder);
      }
      return this;
    }

    Builder authenticatorIfAbsent(Authenticator<Object> authenticator) {
      if (this.authenticator == null) {
        return authenticator(authenticator);
      }
      return this;
    }

    Builder principalMapperIfAbsent(PrincipalMapper<Object, Object> principalMapper) {
      if (this.principalMapper == null) {
        return principalMapper(principalMapper);
      }
      return this;
    }

    Builder loggerIfAbsent(String name, Level level) {
      if (this.logger == null) {
        return logger(name, level);
      }
      return this;
    }

    public ServiceInfo build() {
      return new ServiceInfo(this);
    }
  }
}
