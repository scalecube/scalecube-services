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

  private final JwtTokenResolver tokenResolver;
  private final Retry retryStrategy;

  public ServiceTokenAuthenticator(JwtTokenResolver tokenResolver) {
    this(tokenResolver, Retry.max(0));
  }

  public ServiceTokenAuthenticator(JwtTokenResolver tokenResolver, Retry retryStrategy) {
    this.tokenResolver = tokenResolver;
    this.retryStrategy = retryStrategy;
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
}
