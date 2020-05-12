package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.sut.security.PartiallySecuredService;
import io.scalecube.services.sut.security.PartiallySecuredServiceImpl;
import io.scalecube.services.sut.security.SecuredService;
import io.scalecube.services.sut.security.SecuredServiceImpl;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class ServiceAuthLocalTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static final Map<String, String> CREDENTIALS = new HashMap<>();

  static {
    CREDENTIALS.put("username", "Alice");
    CREDENTIALS.put("password", "qwerty");
  }

  private static final Authenticator authenticator =
      headers -> {
        String username = headers.get("username");
        String password = headers.get("password");

        if ("Alice".equals(username) && "qwerty".equals(password)) {
          return Mono.just(Collections.singletonMap("Alice", "ADMIN"));
        }

        return Mono.error(new UnauthorizedException("Authentication failed"));
      };

  private Microservices service;

  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
  }

  @AfterEach
  void afterEach() {
    if (service != null) {
      service.shutdown().block(TIMEOUT);
    }
  }

  @Test
  @DisplayName("Successful authentication")
  void successfulAuthentication() {
    service =
        Microservices.builder()
            .authenticator(authenticator)
            .services(new SecuredServiceImpl())
            .startAwait();

    SecuredService securedService =
        service.call().credentials(CREDENTIALS).api(SecuredService.class);

    StepVerifier.create(securedService.helloWithRequest("Bob"))
        .assertNext(response -> assertEquals("Hello, Bob", response))
        .verifyComplete();

    StepVerifier.create(securedService.helloWithPrincipal())
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();

    StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob"))
        .assertNext(response -> assertEquals("Hello, Bob and Alice", response))
        .verifyComplete();
  }

  @Test
  @DisplayName("Authentication failed if authenticator not provided")
  void failedAuthenticationWhenAuthenticatorNotProvided() {
    service = Microservices.builder().services(new SecuredServiceImpl()).startAwait();

    SecuredService securedService =
        service.call().credentials(CREDENTIALS).api(SecuredService.class);

    Consumer<Throwable> verifyError =
        th -> {
          assertEquals(UnauthorizedException.class, th.getClass());
          assertEquals("Authenticator not found", th.getMessage());
        };

    StepVerifier.create(securedService.helloWithRequest("Bob"))
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithPrincipal())
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob"))
        .expectErrorSatisfies(verifyError)
        .verify();
  }

  @Test
  @DisplayName("Authentication failed with invalid or empty credentials")
  void failedAuthenticationWithInvalidOrEmptyCredentials() {
    service =
        Microservices.builder()
            .authenticator(authenticator)
            .services(new SecuredServiceImpl())
            .startAwait();

    SecuredService securedService = service.call().api(SecuredService.class);

    Consumer<Throwable> verifyError =
        th -> {
          assertEquals(UnauthorizedException.class, th.getClass());
          assertEquals("Authentication failed", th.getMessage());
        };

    StepVerifier.create(securedService.helloWithRequest("Bob"))
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithPrincipal())
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob"))
        .expectErrorSatisfies(verifyError)
        .verify();
  }

  @Test
  @DisplayName("Successful authentication of partially secured service")
  void successfulAuthenticationOnPartiallySecuredService() {
    service =
        Microservices.builder()
            .authenticator(authenticator)
            .services(new PartiallySecuredServiceImpl())
            .startAwait();

    PartiallySecuredService proxy =
        service.call().credentials(CREDENTIALS).api(PartiallySecuredService.class);

    StepVerifier.create(proxy.securedMethod("Alice"))
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();
  }

  @Test
  @DisplayName("Successful call public method of partially secured service without authentication")
  void successfulCallOfPublicMethodWithoutAuthentication() {
    service = Microservices.builder().services(new PartiallySecuredServiceImpl()).startAwait();

    PartiallySecuredService proxy = service.call().api(PartiallySecuredService.class);

    StepVerifier.create(proxy.publicMethod("Alice"))
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();
  }
}
