package io.scalecube.services.security;

import io.scalecube.security.tokens.jwt.JwtTokenResolver;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.UnauthorizedException;
import java.util.Map;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Implementation of {@link Authenticator} backed by {@link JwtTokenResolver}. Using {@code
 * tokenResolver} (and {@code authDataMapper}) this authenticator turns extracted (and verified)
 * source auth data into client specified destination auth data.
 */
public final class ServiceTokenAuthenticator<T> implements Authenticator<T> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceTokenAuthenticator.class);

  private JwtTokenResolver tokenResolver;
  private ServiceTokenMapper tokenMapper;
  private AuthDataMapper<T> authDataMapper;
  private Retry retryStrategy = RetryStrategies.noRetriesRetryStrategy();

  public ServiceTokenAuthenticator() {}

  private ServiceTokenAuthenticator(ServiceTokenAuthenticator<T> other) {
    this.tokenResolver = other.tokenResolver;
    this.tokenMapper = other.tokenMapper;
    this.authDataMapper = other.authDataMapper;
    this.retryStrategy = other.retryStrategy;
  }

  private ServiceTokenAuthenticator<T> copy() {
    return new ServiceTokenAuthenticator<>(this);
  }

  /**
   * Setter for tokenResolver.
   *
   * @param tokenResolver tokenResolver
   * @return new instance with applied setting
   */
  public ServiceTokenAuthenticator<T> tokenResolver(JwtTokenResolver tokenResolver) {
    final ServiceTokenAuthenticator<T> c = copy();
    c.tokenResolver = tokenResolver;
    return c;
  }

  /**
   * Setter for tokenMapper.
   *
   * @param tokenMapper tokenMapper
   * @return new instance with applied setting
   */
  public ServiceTokenAuthenticator<T> tokenMapper(ServiceTokenMapper tokenMapper) {
    final ServiceTokenAuthenticator<T> c = copy();
    c.tokenMapper = tokenMapper;
    return c;
  }

  /**
   * Setter for authDataMapper.
   *
   * @param authDataMapper authDataMapper
   * @return new instance with applied setting
   */
  public ServiceTokenAuthenticator<T> authDataMapper(AuthDataMapper<T> authDataMapper) {
    final ServiceTokenAuthenticator<T> c = copy();
    c.authDataMapper = authDataMapper;
    return c;
  }

  /**
   * Setter for retryStrategy.
   *
   * @param retryStrategy retryStrategy
   * @return new instance with applied setting
   */
  public ServiceTokenAuthenticator<T> retryStrategy(Retry retryStrategy) {
    final ServiceTokenAuthenticator<T> c = copy();
    c.retryStrategy = retryStrategy;
    return c;
  }

  @Override
  public Mono<T> apply(Map<String, String> credentials) {
    return Mono.defer(
        () -> {
          String serviceToken = tokenMapper.map(credentials);

          if (serviceToken == null) {
            throw new UnauthorizedException("Authentication failed");
          }

          return tokenResolver
              .resolve(serviceToken)
              .map(authDataMapper::map)
              .retryWhen(retryStrategy)
              .doOnError(th -> LOGGER.error("Failed to authenticate, cause: {}", th.toString()))
              .onErrorMap(th -> new UnauthorizedException("Authentication failed"))
              .switchIfEmpty(Mono.error(new UnauthorizedException("Authentication failed")));
        });
  }

  @FunctionalInterface
  public interface ServiceTokenMapper {

    /**
     * Returns service token from input credentials.
     *
     * @param credentials credentials (which must contain service token)
     * @return extracted service token
     */
    String map(Map<String, String> credentials);
  }

  @FunctionalInterface
  public interface AuthDataMapper<A> {

    /**
     * Converts source auth data to the destination auth data.
     *
     * @param authData source auth data
     * @return converted auth data
     */
    A map(Map<String, Object> authData);
  }
}
