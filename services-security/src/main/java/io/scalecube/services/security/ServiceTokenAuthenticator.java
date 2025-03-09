package io.scalecube.services.security;

import io.scalecube.security.tokens.jwt.JwtToken;
import io.scalecube.security.tokens.jwt.JwtTokenResolver;
import io.scalecube.security.tokens.jwt.JwtUnavailableException;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import java.time.Duration;
import java.util.Arrays;
import java.util.stream.Collectors;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

public class ServiceTokenAuthenticator implements Authenticator {

  private final JwtTokenResolver tokenResolver;
  private final Retry retryStrategy;

  public ServiceTokenAuthenticator(JwtTokenResolver tokenResolver) {
    this(tokenResolver, 5, Duration.ofSeconds(3));
  }

  public ServiceTokenAuthenticator(
      JwtTokenResolver tokenResolver, int retryMaxAttempts, Duration retryFixedDelay) {
    this.tokenResolver = tokenResolver;
    this.retryStrategy =
        Retry.fixedDelay(retryMaxAttempts, retryFixedDelay)
            .filter(ex -> ex instanceof JwtUnavailableException);
  }

  @Override
  public Mono<Principal> authenticate(byte[] credentials) {
    return Mono.fromFuture(tokenResolver.resolve(new String(credentials)))
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

              return new ServiceTokenPrincipal(role, permissions);
            });
  }
}
