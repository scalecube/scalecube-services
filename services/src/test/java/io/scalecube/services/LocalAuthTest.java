package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.json.JsonMapper;
import io.rsocket.exceptions.RejectedSetupException;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.sut.security.AnotherSecuredService;
import io.scalecube.services.sut.security.AnotherSecuredServiceImpl;
import io.scalecube.services.sut.security.PartiallySecuredService;
import io.scalecube.services.sut.security.PartiallySecuredServiceImpl;
import io.scalecube.services.sut.security.SecuredService;
import io.scalecube.services.sut.security.SecuredServiceImpl;
import io.scalecube.services.sut.security.UserProfile;
import io.scalecube.services.transport.api.ClientTransport.CredentialsSupplier;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Optional;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

final class LocalAuthTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final LoopResources LOOP_RESOURCE = LoopResources.create("exberry-service-call");

  private static Microservices service;

  @SuppressWarnings("unchecked")
  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(TIMEOUT);

    service =
        Microservices.start(
            new Context()
                .transport(
                    () -> new RSocketServiceTransport().authenticator(LocalAuthTest::authenticate))
                .services(
                    ServiceInfo.fromServiceInstance(new SecuredServiceImpl())
                        .principalMapper(p -> mapPrincipal((Map<String, String>) p))
                        .build(),
                    ServiceInfo.fromServiceInstance(new AnotherSecuredServiceImpl())
                        .principalMapper(p -> mapPrincipal((Map<String, String>) p))
                        .build(),
                    ServiceInfo.fromServiceInstance(new PartiallySecuredServiceImpl())
                        .principalMapper(p -> mapPrincipal((Map<String, String>) p))
                        .build()));
  }

  @AfterAll
  static void afterAll() {
    if (service != null) {
      service.close();
    }
  }

  @Test
  @DisplayName("Successful authentication")
  void successfulAuthentication() {
    try (var caller = serviceCall(LocalAuthTest::credentials)) {
      SecuredService securedService = caller.api(SecuredService.class);

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
  }

  @Test
  @DisplayName("Authentication failed if authenticator not provided")
  void failedAuthenticationWhenAuthenticatorNotProvided() {
    try (var caller = serviceCall(null)) {
      AnotherSecuredService securedService = caller.api(AnotherSecuredService.class);

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
  }

  @Test
  @DisplayName("Authentication failed with empty credentials")
  void failedAuthenticationWithEmptyCredentials() {
    try (var caller = serviceCall(null)) {
      SecuredService securedService = caller.api(SecuredService.class);

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
  }

  @Test
  @DisplayName("Authentication failed with invalid credentials")
  void failedAuthenticationWithInvalidCredentials() {
    try (var caller = serviceCall(LocalAuthTest::invalidCredentials)) {
      SecuredService securedService = caller.api(SecuredService.class);

      Consumer<Throwable> verifyError =
          th -> {
            assertEquals(RejectedSetupException.class, th.getClass());
            assertEquals("Authentication failed (username or password incorrect)", th.getMessage());
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
  }

  @Test
  @DisplayName("Successful authentication of partially secured service")
  void successfulAuthenticationOnPartiallySecuredService() {
    try (var caller = serviceCall(LocalAuthTest::credentials)) {
      StepVerifier.create(caller.api(PartiallySecuredService.class).securedMethod("Alice"))
          .assertNext(response -> assertEquals("Hello, Alice", response))
          .verifyComplete();

      StepVerifier.create(caller.api(PartiallySecuredService.class).publicMethod("Alice"))
          .assertNext(response -> assertEquals("Hello, Alice", response))
          .verifyComplete();
    }
  }

  @Test
  @DisplayName("Successful call public method of partially secured service without authentication")
  void successfulCallOfPublicMethodWithoutAuthentication() {
    try (var caller = serviceCall(LocalAuthTest::credentials)) {
      StepVerifier.create(caller.api(PartiallySecuredService.class).publicMethod("publicMethod"))
          .assertNext(response -> assertEquals("Hello, publicMethod", response))
          .verifyComplete();

      StepVerifier.create(caller.api(PartiallySecuredService.class).publicMethod("securedMethod"))
          .assertNext(response -> assertEquals("Hello, securedMethod", response))
          .verifyComplete();
    }
  }

  @SuppressWarnings("EnhancedSwitchMigration")
  private static Mono<byte[]> credentials(ServiceReference serviceReference) {
    final Map<String, String> credentials = new HashMap<>();
    credentials.put("username", "Alice");
    credentials.put("password", "qwerty");

    switch (serviceReference.namespace()) {
      case "anotherSecured":
      case "secured":
        return Mono.just(encodeCredentials(credentials));
    }

    switch (serviceReference.qualifier()) {
      case "partiallySecured/publicMethod":
        return Mono.just(new byte[0]);
      case "partiallySecured/securedMethod":
        return Mono.just(encodeCredentials(credentials));
    }

    return Mono.just(new byte[0]);
  }

  private static Mono<byte[]> invalidCredentials(ServiceReference serviceReference) {
    final Map<String, String> credentials = new HashMap<>();
    credentials.put("username", "Invalid-User");
    credentials.put("password", "Invalid-Password");
    return Mono.just(encodeCredentials(credentials));
  }

  private static Mono<Map<String, String>> authenticate(Map<String, String> headers) {
    String username = headers.get("username");
    String password = headers.get("password");

    if ("Alice".equals(username) && "qwerty".equals(password)) {
      HashMap<String, String> authData = new HashMap<>();
      authData.put("name", "Alice");
      authData.put("role", "ADMIN");
      return Mono.just(authData);
    }

    return Mono.error(
        new UnauthorizedException("Authentication failed (username or password incorrect)"));
  }

  private static UserProfile mapPrincipal(Map<String, String> authData) {
    return new UserProfile(authData.get("name"), authData.get("role"));
  }

  @SuppressWarnings("resource")
  private static ServiceCall serviceCall(CredentialsSupplier credentialsSupplier) {
    final var loopResources = LOOP_RESOURCE;
    final var address = service.serviceAddress();

    return new ServiceCall()
        .transport(
            new RSocketClientTransport(
                HeadersCodec.DEFAULT_INSTANCE,
                DataCodec.getAllInstances(),
                RSocketClientTransportFactory.websocket().apply(loopResources),
                credentialsSupplier,
                null))
        .router(
            (serviceRegistry, request) -> {
              final var qualifier = request.qualifier();
              final var ind = qualifier.lastIndexOf(Qualifier.DELIMITER);
              final var ns = qualifier.substring(0, ind);
              final var action = qualifier.substring(ind + 1);

              final var serviceReference =
                  new ServiceReference(
                      ServiceMethodDefinition.builder()
                          .action(action)
                          .isSecured(credentialsSupplier != null)
                          .build(),
                      new ServiceRegistration(ns, Collections.emptyMap(), Collections.emptyList()),
                      ServiceEndpoint.builder()
                          .id(UUID.randomUUID().toString())
                          .address(address)
                          .build());

              return Optional.of(serviceReference);
            });
  }

  private static byte[] encodeCredentials(Map<String, String> credentials) {
    try {
      return new JsonMapper().writeValueAsBytes(credentials);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }
}
