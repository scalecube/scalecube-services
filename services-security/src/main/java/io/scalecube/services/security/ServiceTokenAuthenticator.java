package io.scalecube.services.security;

import io.scalecube.security.jwt.JwksTokenResolver;
import io.scalecube.security.jwt.JwtToken;
import io.scalecube.security.jwt.JwtUnavailableException;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.auth.ServicePrincipal;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

/**
 * Service authenticator based on JWT token. Being used to verify service identity that establishing
 * session to the system. JWT token must have claims: {@code role} and {@code permissions}.
 */
public class ServiceTokenAuthenticator implements Authenticator {

  private final JwksTokenResolver tokenResolver;
  private final Retry retryStrategy;

  /**
   * Constructor with defaults.
   *
   * @param tokenResolver token resolver
   */
  public ServiceTokenAuthenticator(JwksTokenResolver tokenResolver) {
    this(tokenResolver, 5, Duration.ofSeconds(3));
  }

  /**
   * Constructor.
   *
   * @param tokenResolver token resolver
   * @param retryMaxAttempts max number of retry attempts
   * @param retryFixedDelay delay between retry attempts
   */
  public ServiceTokenAuthenticator(
      JwksTokenResolver tokenResolver, int retryMaxAttempts, Duration retryFixedDelay) {
    this.tokenResolver = tokenResolver;
    this.retryStrategy =
        Retry.fixedDelay(retryMaxAttempts, retryFixedDelay)
            .filter(ex -> ex instanceof JwtUnavailableException);
  }

  @Override
  public Mono<Principal> authenticate(byte[] credentials) {
    return Mono.fromFuture(tokenResolver.resolveToken(new String(credentials)))
        .retryWhen(retryStrategy)
        .map(JwtToken::payload)
        .map(
            payload -> {
              final var role = (String) payload.get("role");
              if (role == null) {
                throw new IllegalArgumentException("Wrong token: role claim is missing");
              }

              final var permissionsClaim = (String) payload.get("permissions");
              if (permissionsClaim == null) {
                throw new IllegalArgumentException("Wrong token: permissions claim is missing");
              }

              final var permissions =
                  Arrays.stream(permissionsClaim.split(","))
                      .map(String::trim)
                      .filter(s -> !s.isBlank())
                      .collect(Collectors.toSet());

              return new ServicePrincipal(role, permissions);
            });
  }
}
