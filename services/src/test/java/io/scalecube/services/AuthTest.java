package io.scalecube.services;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.params.provider.Arguments.arguments;

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
import io.scalecube.services.sut.security.CompositeSecuredService;
import io.scalecube.services.sut.security.CompositeSecuredServiceImpl;
import io.scalecube.services.sut.security.CompositeSecuredServiceImpl.CompositePrincipalImpl;
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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.StringJoiner;
import java.util.UUID;
import java.util.function.Function;
import java.util.stream.Stream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.Arguments;
import org.junit.jupiter.params.provider.MethodSource;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

final class AuthTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);
  private static final LoopResources LOOP_RESOURCE = LoopResources.create("service-call");
  private static final ObjectMapper OBJECT_MAPPER = initMapper();
  private static final String VALID_TOKEN = UUID.randomUUID().toString();
  private static final String INVALID_TOKEN = UUID.randomUUID().toString();

  private static Microservices service;
  private ServiceCall serviceCall;

  @BeforeAll
  static void beforeAll() {
    StepVerifier.setDefaultTimeout(TIMEOUT);
    service =
        Microservices.start(
            new Context()
                .defaultLogger("AuthTest")
                .transport(
                    () ->
                        new RSocketServiceTransport()
                            .authenticator(AuthTest::authenticateServiceTransport))
                .services(new SecuredServiceImpl())
                .services(
                    ServiceInfo.fromServiceInstance(new CompositeSecuredServiceImpl())
                        .authenticator(AuthTest::authenticateComposite)
                        .build()));
  }

  @AfterAll
  static void afterAll() {
    StepVerifier.resetDefaultTimeout();
    if (service != null) {
      service.close();
    }
  }

  @AfterEach
  void afterEach() {
    if (serviceCall != null) {
      serviceCall.close();
    }
  }

  @Nested
  class SecuredTests {

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("authenticateSuccessfullyMethodSource")
    void authenticateSuccessfully(String test, SuccessArgs args) {
      serviceCall =
          serviceCall(
              ($, serviceRole) -> credentials(args.tokenSupplier.apply(serviceRole)),
              args.serviceRole,
              args.allowedRoles);

      StepVerifier.create(serviceCall.api(SecuredService.class).invokeWithRoleOrPermissions())
          .verifyComplete();
    }

    private record SuccessArgs(
        Function<String, TokenCredentials> tokenSupplier,
        String serviceRole,
        List<String> allowedRoles) {}

    private static Stream<Arguments> authenticateSuccessfullyMethodSource() {
      return Stream.of(
          arguments(
              "Authenticate by service role only",
              new SuccessArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, null),
                  "invoker",
                  null)),
          arguments(
              "Authenticate by permissions only",
              new SuccessArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, List.of("invoke")),
                  null,
                  null)),
          arguments(
              "Authenticate with allowed service role",
              new SuccessArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, List.of("invoke")),
                  "caller",
                  List.of("invoker", "caller", "admin"))));
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("failedAuthenticationSource")
    void failedAuthentication(String test, FailedArgs args) {
      serviceCall =
          serviceCall(
              ($, serviceRole) -> credentials(args.tokenSupplier.apply(serviceRole)),
              args.serviceRole,
              args.allowedRoles);

      StepVerifier.create(serviceCall.api(SecuredService.class).invokeWithRoleOrPermissions())
          .verifyErrorSatisfies(
              ex -> {
                assertInstanceOf(args.errorClass, ex, "errorClass");
                assertEquals(args.errorMessage, ex.getMessage(), "errorMessage");
              });
    }

    private record FailedArgs(
        Function<String, TokenCredentials> tokenSupplier,
        String serviceRole,
        List<String> allowedRoles,
        Class<? extends Throwable> errorClass,
        String errorMessage) {}

    private static Stream<Arguments> failedAuthenticationSource() {
      return Stream.of(
          arguments(
              "Failed to authenticate: invalid token",
              new FailedArgs(
                  serviceRole -> new TokenCredentials(INVALID_TOKEN, serviceRole, null),
                  null,
                  null,
                  RejectedSetupException.class,
                  "Authentication failed")),
          arguments(
              "Failed to authenticate: service role is null, permissions is null",
              new FailedArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, null),
                  null,
                  null,
                  ForbiddenException.class,
                  "Insufficient permissions")),
          arguments(
              "Failed to authenticate: wrong service role",
              new FailedArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, null),
                  "wrong",
                  null,
                  ForbiddenException.class,
                  "Insufficient permissions")),
          arguments(
              "Failed to authenticate: wrong permissions",
              new FailedArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, List.of("wrong")),
                  null,
                  null,
                  ForbiddenException.class,
                  "Insufficient permissions")),
          arguments(
              "Failed to authenticate: service role is not allowed",
              new FailedArgs(
                  serviceRole -> new TokenCredentials(VALID_TOKEN, serviceRole, null),
                  "foo",
                  List.of("invoker", "caller"),
                  ForbiddenException.class,
                  "Insufficient permissions")));
    }
  }

  @Nested
  class CompositeSecuredTests {

    @Test
    void authenticateSuccessfully() {
      final var role = "invoker";
      final var tokenCredentials = new TokenCredentials(VALID_TOKEN, role, null);

      final var credentials = new HashMap<String, String>();
      credentials.put("user", "alice");
      credentials.put("permissions", "helloComposite");

      serviceCall = serviceCall((sr, r) -> credentials(tokenCredentials), role, null);

      StepVerifier.create(
              serviceCall
                  .credentials(credentials)
                  .api(CompositeSecuredService.class)
                  .helloComposite())
          .verifyComplete();
    }

    @ParameterizedTest(name = "[{index}] {0}")
    @MethodSource("failedAuthenticationSource")
    void failedAuthentication(String test, FailedArgs args) {
      final var role = "invoker";
      final var tokenCredentials = new TokenCredentials(VALID_TOKEN, role, null);

      serviceCall = serviceCall((sr, r) -> credentials(tokenCredentials), role, null);

      StepVerifier.create(
              serviceCall
                  .credentials(args.credentials)
                  .api(CompositeSecuredService.class)
                  .helloComposite())
          .verifyErrorSatisfies(
              ex -> {
                assertInstanceOf(args.errorClass, ex, "errorClass");
                assertEquals(args.errorMessage, ex.getMessage(), "errorMessage");
              });
    }

    private record FailedArgs(
        Map<String, String> credentials,
        Class<? extends Throwable> errorClass,
        String errorMessage) {}

    private static Stream<Arguments> failedAuthenticationSource() {
      return Stream.of(
          arguments(
              "Authentication failed: empty composite credentails",
              new FailedArgs(Map.of(), UnauthorizedException.class, "Not allowed")),
          arguments(
              "Authentication failed: wrong composite user",
              new FailedArgs(
                  Map.of("user", "wrong-user"), UnauthorizedException.class, "Not allowed")),
          arguments(
              "Authentication failed: wrong composite permissions",
              new FailedArgs(
                  Map.of("user", "alice", "permissions", "wrong-permissions"),
                  ForbiddenException.class,
                  "Insufficient permissions")));
    }
  }

  @Nested
  class ServiceRoleSecuredTests {

    @Test
    void authenticateSuccessfully() {
      final var role = "admin";
      final var tokenCredentials =
          new TokenCredentials(VALID_TOKEN, role, List.of("read", "write"));

      serviceCall = serviceCall((sr, r) -> credentials(tokenCredentials), role, null);

      StepVerifier.create(serviceCall.api(SecuredService.class).invokeWithAllowedRoleAnnotation())
          .verifyComplete();
    }
  }

  private static Mono<byte[]> credentials(TokenCredentials tokenCredentials) {
    return Mono.fromCallable(
        () -> {
          try {
            return OBJECT_MAPPER.writeValueAsBytes(tokenCredentials);
          } catch (JsonProcessingException e) {
            throw new RuntimeException(e);
          }
        });
  }

  private static Mono<Principal> authenticateServiceTransport(byte[] credentials) {
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

  private static Mono<Principal> authenticateComposite(RequestContext requestContext) {
    return RequestContext.deferContextual()
        .map(
            context -> {
              if (!context.hasPrincipal()) {
                throw new ForbiddenException("Insufficient permissions");
              }

              final var headers = context.headers();
              final var user = headers.get("user");
              final var permissions = headers.get("permissions");

              if (user == null || permissions == null) {
                throw new ForbiddenException("Not allowed");
              }
              if (!"alice".equals(user)) {
                throw new ForbiddenException("Not allowed");
              }

              return new CompositePrincipalImpl(context.principal(), List.of(permissions));
            });
  }

  private ServiceCall serviceCall(
      CredentialsSupplier credentialsSupplier, String serviceRole, List<String> allowedRoles) {
    //noinspection resource
    return new ServiceCall()
        .transport(
            new RSocketClientTransport(
                HeadersCodec.DEFAULT_INSTANCE,
                DataCodec.getAllInstances(),
                RSocketClientTransportFactory.websocket().apply(LOOP_RESOURCE),
                credentialsSupplier,
                allowedRoles))
        .router(
            StaticAddressRouter.from(service.serviceAddress())
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

    @SuppressWarnings("unused")
    public TokenCredentials() {}

    public TokenCredentials(String token, String serviceRole, List<String> permissions) {
      this.token = token;
      this.serviceRole = serviceRole;
      this.permissions = permissions;
    }

    public String token() {
      return token;
    }

    public String serviceRole() {
      return serviceRole;
    }

    public List<String> permissions() {
      return permissions;
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
