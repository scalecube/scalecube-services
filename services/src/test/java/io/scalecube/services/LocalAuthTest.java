package io.scalecube.services;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.Matchers.startsWith;
import static org.junit.jupiter.api.Assertions.assertEquals;

import com.fasterxml.jackson.annotation.JsonAutoDetect;
import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonTypeInfo;
import com.fasterxml.jackson.annotation.PropertyAccessor;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.ObjectMapper.DefaultTyping;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.databind.jsontype.impl.LaissezFaireSubTypeValidator;
import io.rsocket.exceptions.RejectedSetupException;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.ForbiddenException;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.sut.security.PartiallySecuredService;
import io.scalecube.services.sut.security.PartiallySecuredServiceImpl;
import io.scalecube.services.sut.security.SecuredService;
import io.scalecube.services.sut.security.SecuredServiceImpl;
import io.scalecube.services.transport.api.ClientTransport.CredentialsSupplier;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.io.IOException;
import java.time.Duration;
import java.util.List;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Consumer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.ValueSource;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

final class LocalAuthTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final LoopResources LOOP_RESOURCE = LoopResources.create("exberry-service-call");
  private static final ObjectMapper OBJECT_MAPPER = initMapper();
  private static final String VALID_TOKEN = UUID.randomUUID().toString();

  private Microservices service;

  @BeforeEach
  void beforeEach() {
    StepVerifier.setDefaultTimeout(TIMEOUT);

    service =
        Microservices.start(
            new Context()
                .transport(
                    () -> new RSocketServiceTransport().authenticator(LocalAuthTest::authenticate))
                .services(new SecuredServiceImpl(), new PartiallySecuredServiceImpl()));
  }

  @AfterEach
  void afterEach() {
    if (service != null) {
      service.close();
    }
  }

  @Test
  @DisplayName("Successfull authentication")
  void successfulAuthentication() {
    try (var caller =
        serviceCall((serviceReference, serviceRole) -> credentials(serviceReference, "ADMIN"))) {
      final var securedService = caller.api(SecuredService.class);

      StepVerifier.create(securedService.helloWithRequest("Bob"))
          .assertNext(response -> assertEquals("Hello, Bob", response))
          .verifyComplete();

      StepVerifier.create(securedService.helloWithPrincipal())
          .assertNext(response -> assertThat(response, startsWith("Hello | ")))
          .verifyComplete();

      StepVerifier.create(securedService.helloWithRequestAndPrincipal("Bob"))
          .assertNext(response -> assertEquals("Hello, Bob", response))
          .verifyComplete();
    }
  }

  @Test
  @DisplayName("Authentication failed with empty credentials")
  void failedAuthenticationWithEmptyCredentials() {
    try (var caller = serviceCall(LocalAuthTest::emptyCredentials)) {
      final var securedService = caller.api(SecuredService.class);

      Consumer<Throwable> verifyError =
          th -> {
            assertEquals(RejectedSetupException.class, th.getClass());
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
      final var securedService = caller.api(SecuredService.class);

      Consumer<Throwable> verifyError =
          th -> {
            assertEquals(RejectedSetupException.class, th.getClass());
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
  @DisplayName("Successfull authentication of partially secured service")
  void successfulAuthenticationOnPartiallySecuredService() {
    try (var caller = serviceCall(LocalAuthTest::credentials)) {
      StepVerifier.create(caller.api(PartiallySecuredService.class).securedMethod("securedMethod"))
          .assertNext(response -> assertEquals("Hello, securedMethod", response))
          .verifyComplete();

      StepVerifier.create(caller.api(PartiallySecuredService.class).publicMethod("publicMethod"))
          .assertNext(response -> assertEquals("Hello, publicMethod", response))
          .verifyComplete();
    }
  }

  @Test
  @DisplayName("Successfull call public method of partially secured service without authentication")
  void successfulCallOfPublicMethodWithoutAuthentication() {
    try (var caller = serviceCall(LocalAuthTest::credentials)) {
      StepVerifier.create(caller.api(PartiallySecuredService.class).publicMethod("publicMethod"))
          .assertNext(response -> assertEquals("Hello, publicMethod", response))
          .verifyComplete();

      StepVerifier.create(caller.api(PartiallySecuredService.class).securedMethod("securedMethod"))
          .assertNext(response -> assertEquals("Hello, securedMethod", response))
          .verifyComplete();
    }
  }

  @ValueSource(strings = {"helloRole1", "helloRole2"})
  @ParameterizedTest
  @DisplayName("Successfull call of method with service role")
  void successfulCallOfMethodWithRole(String role) {
    try (var caller = serviceCall((serviceReference, r) -> credentials(serviceReference, role))) {
      StepVerifier.create(caller.api(SecuredService.class).helloWithRoles("Alice"))
          .assertNext(response -> assertEquals("Hello, Alice", response))
          .verifyComplete();
    }
  }

  @Test
  @DisplayName("Failed to call of method with service role")
  void failedCallMethodWithRole() {
    final String role = "wrong-role-" + System.currentTimeMillis();
    try (var caller = serviceCall((serviceReference, r) -> credentials(serviceReference, role))) {
      StepVerifier.create(caller.api(SecuredService.class).helloWithRoles("Bob"))
          .expectErrorSatisfies(
              th -> {
                assertEquals(ForbiddenException.class, th.getClass());
                assertEquals("Forbidden", th.getMessage());
              })
          .verify();
    }
  }

  private static Mono<byte[]> credentials(ServiceReference serviceReference, String serviceRole) {
    return Mono.just(
        encodeCredentials(new TokenCredentials().token(VALID_TOKEN).serviceRole(serviceRole)));
  }

  private static Mono<byte[]> invalidCredentials(
      ServiceReference serviceReference, String serviceRole) {
    return Mono.just(
        encodeCredentials(
            new TokenCredentials().token(UUID.randomUUID().toString()).serviceRole(serviceRole)));
  }

  private static Mono<byte[]> emptyCredentials(
      ServiceReference serviceReference, String serviceRole) {
    return Mono.just(encodeCredentials(new TokenCredentials()));
  }

  private static Mono<Principal> authenticate(byte[] credentials) {
    if (credentials.length == 0) {
      return Mono.just(NULL_PRINCIPAL);
    }

    final var tokenCredentials = credentials(credentials, TokenCredentials.class);
    final var token = tokenCredentials.token();
    final var serviceRole = tokenCredentials.serviceRole();
    final var permissions = tokenCredentials.permissions();

    if (VALID_TOKEN.equals(token)) {
      return Mono.just(new PrincipalImpl(serviceRole, permissions));
    }

    return Mono.error(new UnauthorizedException("Authentication failed"));
  }

  private ServiceCall serviceCall(CredentialsSupplier credentialsSupplier) {
    return serviceCall(credentialsSupplier, null);
  }

  private ServiceCall serviceCall(CredentialsSupplier credentialsSupplier, String serviceRole) {
    //noinspection resource
    return new ServiceCall()
        .transport(
            new RSocketClientTransport(
                HeadersCodec.DEFAULT_INSTANCE,
                DataCodec.getAllInstances(),
                RSocketClientTransportFactory.websocket().apply(LOOP_RESOURCE),
                credentialsSupplier,
                null))
        .router(
            StaticAddressRouter.builder()
                .address(service.serviceAddress())
                .secured(credentialsSupplier != null)
                .serviceRole(serviceRole)
                .build());
  }

  private static ObjectMapper initMapper() {
    final var mapper = new ObjectMapper();
    mapper.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    mapper.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
    mapper.configure(DeserializationFeature.READ_UNKNOWN_ENUM_VALUES_AS_NULL, true);
    mapper.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
    mapper.setVisibility(PropertyAccessor.ALL, JsonAutoDetect.Visibility.ANY);
    mapper.setSerializationInclusion(JsonInclude.Include.NON_NULL);
    mapper.configure(SerializationFeature.WRITE_ENUMS_USING_TO_STRING, true);
    mapper.activateDefaultTyping(
        LaissezFaireSubTypeValidator.instance,
        DefaultTyping.JAVA_LANG_OBJECT,
        JsonTypeInfo.As.WRAPPER_OBJECT);
    mapper.findAndRegisterModules();
    return mapper;
  }

  private static byte[] encodeCredentials(Object credentials) {
    try {
      return OBJECT_MAPPER.writeValueAsBytes(credentials);
    } catch (JsonProcessingException e) {
      throw new RuntimeException(e);
    }
  }

  private static <T> T credentials(byte[] credentials, Class<T> clazz) {
    try {
      return OBJECT_MAPPER.readValue(credentials, clazz);
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private static class TokenCredentials {

    private String token;
    private String serviceRole;
    private List<String> permissions;

    public String token() {
      return token;
    }

    public TokenCredentials token(String token) {
      this.token = token;
      return this;
    }

    public String serviceRole() {
      return serviceRole;
    }

    public TokenCredentials serviceRole(String serviceRole) {
      this.serviceRole = serviceRole;
      return this;
    }

    public List<String> permissions() {
      return permissions;
    }

    public TokenCredentials permissions(List<String> permissions) {
      this.permissions = permissions;
      return this;
    }

    @Override
    public String toString() {
      return new StringJoiner(", ", TokenCredentials.class.getSimpleName() + "[", "]")
          .add("token='" + token + "'")
          .add("serviceRole='" + serviceRole + "'")
          .add("permissions=" + permissions)
          .toString();
    }
  }
}
