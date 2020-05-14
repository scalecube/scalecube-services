package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
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

final class ServiceAuthRemoteTest extends BaseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private static final Map<String, String> CREDENTIALS = new HashMap<>();

  static {
    CREDENTIALS.put("username", "Alice");
    CREDENTIALS.put("password", "qwerty");
  }

  private static final Authenticator<Map<String, String>> authenticator =
      headers -> {
        String username = headers.get("username");
        String password = headers.get("password");

        if ("Alice".equals(username) && "qwerty".equals(password)) {
          HashMap<String, String> authData = new HashMap<>();
          authData.put("name", "Alice");
          authData.put("role", "ADMIN");
          return Mono.just(authData);
        }

        return Mono.error(new UnauthorizedException("Authentication failed"));
      };

  private static Microservices caller;
  private static Microservices service;
  public static PrincipalMapper<Map<String, String>, UserProfile> principalMapper;

  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(TIMEOUT);

    caller =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    principalMapper = authData -> new UserProfile(authData.get("name"), authData.get("role"));

    service =
        Microservices.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .defaultAuthenticator(authenticator)
            .services(
                ServiceInfo.fromServiceInstance(new SecuredServiceImpl())
                    .principalMapper(principalMapper)
                    .build())
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
        caller.call().credentials(CREDENTIALS).api(SecuredService.class);

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
    Microservices service =
        Microservices.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .services(
                ServiceInfo.fromServiceInstance(new SecuredServiceImpl())
                    .principalMapper(principalMapper)
                    .build())
            .startAwait();

    SecuredService securedService =
        caller.call().credentials(CREDENTIALS).api(SecuredService.class);

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

    service.shutdown().block(TIMEOUT);
  }

  @Test
  @DisplayName("Authentication failed with invalid or empty credentials")
  void failedAuthenticationWithInvalidOrEmptyCredentials() {
    SecuredService securedService = caller.call().api(SecuredService.class);

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
    Microservices service =
        Microservices.builder()
            .discovery(ServiceAuthRemoteTest::serviceDiscovery)
            .transport(RSocketServiceTransport::new)
            .defaultAuthenticator(authenticator)
            .services(
                ServiceInfo.fromServiceInstance(new PartiallySecuredServiceImpl())
                    .principalMapper(principalMapper)
                    .build())
            .startAwait();

    PartiallySecuredService proxy =
        caller.call().credentials(CREDENTIALS).api(PartiallySecuredService.class);

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
            .services(
                ServiceInfo.fromServiceInstance(new PartiallySecuredServiceImpl())
                    .principalMapper(principalMapper)
                    .build())
            .startAwait();

    PartiallySecuredService proxy = caller.call().api(PartiallySecuredService.class);

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
