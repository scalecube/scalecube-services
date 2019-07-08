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
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

final class ServiceAuthRemoteTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static final String CREDENTIALS = "valid_credentials";
  private static final Authenticator<String, UserProfile> authenticator =
      credentials -> {
        if (CREDENTIALS.equals(credentials)) {
          return Mono.just(new UserProfile("Alice", "ADMIN"));
        }

        return Mono.error(new UnauthorizedException("Authentication failed"));
      };

  private static Microservices caller;
  private static Microservices service;

  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(TIMEOUT);

    caller =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    service =
        Microservices.builder()
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
    SecuredService securedService = service.call().api(SecuredService.class, CREDENTIALS);

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
    Microservices service =
        Microservices.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(new SecuredServiceImpl())
            .startAwait();

    SecuredService securedService = service.call().api(SecuredService.class, "invalid_credentials");

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
  @DisplayName("Authentication failed if credentials not provided")
  void failedAuthenticationWhenCredentialsNotProvided() {
    SecuredService securedService = service.call().api(SecuredService.class, null);

    Consumer<Throwable> verifyError =
        th -> {
          assertEquals(UnauthorizedException.class, th.getClass());
          assertEquals("Credentials not found", th.getMessage());
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
  @DisplayName("Authentication failed with invalid credentials")
  void failedAuthenticationWithInvalidCredentials() {
    SecuredService securedService = service.call().api(SecuredService.class, "invalid_credentials");

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
    Microservices service =
        Microservices.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .authenticator(authenticator)
            .services(new PartiallySecuredServiceImpl())
            .startAwait();

    PartiallySecuredService proxy = service.call().api(PartiallySecuredService.class, CREDENTIALS);

    StepVerifier.create(proxy.securedMethod("Alice"))
        .assertNext(response -> assertEquals("Hello, Alice", response))
        .verifyComplete();

    service.shutdown().block(TIMEOUT);
  }

  @Test
  @DisplayName("Successful call public method of partially secured service without authentication")
  void successfulCallOfPublicMethodWithoutAuthentication() {
    Microservices service =
        Microservices.builder()
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

  private static ServiceDiscovery serviceDiscovery(ServiceEndpoint serviceEndpoint) {
    return new ScalecubeServiceDiscovery(serviceEndpoint)
        .options(opts -> opts.seedMembers(caller.discovery().address()));
  }
}
