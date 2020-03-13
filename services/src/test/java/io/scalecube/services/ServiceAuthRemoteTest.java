package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.sut.security.PartiallySecuredService;
import io.scalecube.services.sut.security.PartiallySecuredServiceImpl;
import io.scalecube.services.sut.security.SecuredService;
import io.scalecube.services.sut.security.SecuredServiceImpl;
import io.scalecube.services.sut.security.UserProfile;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class ServiceAuthRemoteTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static final Map<String, String> CREDENTIALS =
      new HashMap<String, String>() {
        {
          put("username", "Alice");
          put("password", "qwerty");
        }
      };

  private static final Authenticator<UserProfile> authenticator =
      message -> {
        Map<String, String> headers = message.headers();
        String username = headers.get("username");
        String password = headers.get("password");

        if ("Alice".equals(username) && "qwerty".equals(password)) {
          return Mono.just(new UserProfile("Alice", "ADMIN"));
        }

        return Mono.error(new UnauthorizedException("Authentication failed"));
      };

  private static Scalecube caller;
  private static Scalecube service;

  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(TIMEOUT);

    caller =
        Scalecube.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    service =
        Scalecube.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .authenticator(authenticator)
            .services(new SecuredServiceImpl())
            .startAwait();
  }

  @AfterAll
  static void afterAll() {
    if (caller != null) {
      caller.shutdown().block(TIMEOUT);
    }

    if (service != null) {
      service.shutdown().block(TIMEOUT);
    }
  }

  @Test
  @DisplayName("Successful authentication")
  void successfulAuthentication() {
    SecuredService securedService =
        service.call().credentials(CREDENTIALS).api(SecuredService.class);

    StepVerifier.create(securedService.helloWithRequest("Bob"))
        .assertNext(response -> assertEquals("Hello, Bob", response))
        .verifyComplete();

    StepVerifier.create(securedService.helloWithPrincipal(null))
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();

    StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob", null))
        .assertNext(response -> assertEquals("Hello, Bob and Alice", response))
        .verifyComplete();
  }

  @Test
  @DisplayName("Authentication failed if authenticator not provided")
  void failedAuthenticationWhenAuthenticatorNotProvided() {
    Scalecube service =
        Scalecube.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(new SecuredServiceImpl())
            .startAwait();

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

    StepVerifier.create(securedService.helloWithPrincipal(null))
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob", null))
        .expectErrorSatisfies(verifyError)
        .verify();

    service.shutdown().block(TIMEOUT);
  }

  @Test
  @DisplayName("Authentication failed with invalid or empty credentials")
  void failedAuthenticationWithInvalidOrEmptyCredentials() {
    SecuredService securedService = service.call().api(SecuredService.class);

    Consumer<Throwable> verifyError =
        th -> {
          assertEquals(UnauthorizedException.class, th.getClass());
          assertEquals("Authentication failed", th.getMessage());
        };

    StepVerifier.create(securedService.helloWithRequest("Bob"))
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithPrincipal(null))
        .expectErrorSatisfies(verifyError)
        .verify();

    StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob", null))
        .expectErrorSatisfies(verifyError)
        .verify();
  }

  @Test
  @DisplayName("Successful authentication of partially secured service")
  void successfulAuthenticationOnPartiallySecuredService() {
    Scalecube service =
        Scalecube.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .authenticator(authenticator)
            .services(new PartiallySecuredServiceImpl())
            .startAwait();

    PartiallySecuredService proxy =
        service.call().credentials(CREDENTIALS).api(PartiallySecuredService.class);

    StepVerifier.create(proxy.securedMethod("Alice"))
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();

    service.shutdown().block(TIMEOUT);
  }

  @Test
  @DisplayName("Successful call public method of partially secured service without authentication")
  void successfulCallOfPublicMethodWithoutAuthentication() {
    Scalecube service =
        Scalecube.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(new PartiallySecuredServiceImpl())
            .startAwait();

    PartiallySecuredService proxy = service.call().api(PartiallySecuredService.class);

    StepVerifier.create(proxy.publicMethod("Alice"))
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();

    service.shutdown().block(TIMEOUT);
  }

  private static ServiceDiscovery serviceDiscovery(ServiceEndpoint endpoint) {
    return new ScalecubeServiceDiscovery(endpoint)
        .membership(cfg -> cfg.seedMembers(caller.discovery().address()));
  }
}
