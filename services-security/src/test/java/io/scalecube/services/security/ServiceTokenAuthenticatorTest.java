package io.scalecube.services.security;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

import io.scalecube.security.jwt.JwtToken;
import io.scalecube.security.jwt.JwtTokenException;
import io.scalecube.security.jwt.JwtTokenResolver;
import io.scalecube.security.jwt.JwtUnavailableException;
import io.scalecube.services.auth.ServicePrincipal;
import java.time.Duration;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.Test;

class ServiceTokenAuthenticatorTest {

  private static final JwtToken TOKEN =
      new JwtToken(Map.of(), Map.of("role", "service", "permissions", "read,write"));

  @Test
  void testRetryOnUnavailable() {
    final var invocations = new AtomicInteger();
    final JwtTokenResolver tokenResolver =
        token ->
            invocations.incrementAndGet() < 3
                ? CompletableFuture.failedFuture(new JwtUnavailableException("unavailable"))
                : CompletableFuture.completedFuture(TOKEN);

    final var principal =
        (ServicePrincipal)
            new ServiceTokenAuthenticator(tokenResolver, 5, Duration.ofMillis(10))
                .authenticate("token".getBytes())
                .block(Duration.ofSeconds(3));

    assertEquals(3, invocations.get());
    assertEquals("service", principal.role());
    assertEquals(Set.of("read", "write"), principal.permissions());
  }

  @Test
  void testNoRetryOnInvalidToken() {
    final var invocations = new AtomicInteger();
    final JwtTokenResolver tokenResolver =
        token -> {
          invocations.incrementAndGet();
          return CompletableFuture.failedFuture(new JwtTokenException("invalid"));
        };

    final var authenticator =
        new ServiceTokenAuthenticator(tokenResolver, 5, Duration.ofMillis(10));

    assertThrows(
        JwtTokenException.class,
        () -> authenticator.authenticate("token".getBytes()).block(Duration.ofSeconds(3)));
    assertEquals(1, invocations.get());
  }
}
