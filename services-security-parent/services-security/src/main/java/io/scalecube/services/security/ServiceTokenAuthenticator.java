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
 * Implementation of {@link Authenticator} backed up by provided {@link JwtTokenResolver}. Using
 * token resolver this authenticator turns extracted (and verified) token claims into the {@link
 * ServiceClaims} object.
 *
 * @see #apply(Map)
 * @see ServiceTokens
 * @see ServiceClaims
 */
public final class ServiceTokenAuthenticator implements Authenticator<ServiceClaims> {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceTokenAuthenticator.class);

  private JwtTokenResolver tokenResolver;
  private Retry retryStrategy = RetryStrategies.noRetriesRetryStrategy();

  public ServiceTokenAuthenticator() {}

  private ServiceTokenAuthenticator(ServiceTokenAuthenticator other) {
    this.tokenResolver = other.tokenResolver;
    this.retryStrategy = other.retryStrategy;
  }

  /**
   * Setter for tokenResolver.
   *
   * @param tokenResolver tokenResolver
   * @return new instance with applied setting
   */
  public ServiceTokenAuthenticator tokenResolver(JwtTokenResolver tokenResolver) {
    final ServiceTokenAuthenticator c = copy();
    c.tokenResolver = tokenResolver;
    return c;
  }

  /**
   * Setter for retryStrategy.
   *
   * @param retryStrategy retryStrategy
   * @return new instance with applied setting
   */
  public ServiceTokenAuthenticator retryStrategy(Retry retryStrategy) {
    final ServiceTokenAuthenticator c = copy();
    c.retryStrategy = retryStrategy;
    return c;
  }

  @Override
  public Mono<ServiceClaims> apply(Map<String, String> credentials) {
    return Mono.defer(
        () -> {
          String serviceToken = credentials.get(ServiceTokens.SERVICE_TOKEN_HEADER);

          if (serviceToken == null) {
            throw new UnauthorizedException("Authentication failed");
          }

          return tokenResolver
              .resolve(serviceToken)
              .map(ServiceTokenAuthenticator::toServiceClaims)
              .retryWhen(retryStrategy)
              .doOnError(th -> LOGGER.error("Failed to authenticate, cause: {}", th.toString()))
              .onErrorMap(th -> new UnauthorizedException("Authentication failed"))
              .switchIfEmpty(Mono.error(new UnauthorizedException("Authentication failed")));
        });
  }

  private static ServiceClaims toServiceClaims(Map<String, Object> authData) {
    String permissionsClaim = (String) authData.get(ServiceTokens.PERMISSIONS_CLAIM);
    if (permissionsClaim == null || permissionsClaim.isEmpty()) {
      throw new UnauthorizedException("Authentication failed");
    }
    return new ServiceClaims(permissionsClaim);
  }

  private ServiceTokenAuthenticator copy() {
    return new ServiceTokenAuthenticator(this);
  }
}
