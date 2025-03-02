package io.scalecube.services.methods;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import io.scalecube.services.Reflect;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.Method;
import java.util.List;
import java.util.function.Consumer;
import java.util.stream.Stream;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;
import org.mockito.ArgumentMatchers;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

class ServiceMethodInvokerTest {

  private static final PrincipalImpl PRINCIPAL = new PrincipalImpl("user", List.of("permission"));

  private final ServiceMessageDataDecoder dataDecoder = (message, type) -> message;
  private final Authenticator authenticator = requestContext -> Mono.just(PRINCIPAL);
  private final StubService stubService = new StubServiceImpl();

  private ServiceMethodInvoker serviceMethodInvoker;

  @Nested
  class InvocationTests {

    @Test
    @DisplayName("invokeOne() should return empty response when service returns null")
    void invokeOneReturnsNull() throws Exception {
      final var methodName = "invokeOneReturnsNull";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .verifyComplete();
    }

    @Test
    @DisplayName("invokeMany() should return empty response when service returns null")
    void invokeManyReturnsNull() throws Exception {
      final var methodName = "invokeManyReturnsNull";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeMany(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .verifyComplete();
    }

    @Test
    @DisplayName("invokeBidirectional() should return empty response when service returns null")
    void invokeBidirectionalReturnsNull() throws Exception {
      final var methodName = "invokeBidirectionalReturnsNull";
      final var method = StubService.class.getMethod(methodName, Flux.class);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeBidirectional(Flux.just(message))
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .verifyComplete();
    }

    @Test
    @DisplayName("invokeOne() should return error response when service throws exception")
    void invokeOneThrowsException() throws Exception {
      final var methodName = "invokeOneThrowsException";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .assertNext(assertError(500, "Error"))
          .verifyComplete();
    }

    @Test
    @DisplayName("invokeMany() should return error response when service throws exception")
    void invokeManyThrowsException() throws Exception {
      final var methodName = "invokeManyThrowsException";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeMany(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .assertNext(assertError(500, "Error"))
          .verifyComplete();
    }

    @Test
    @DisplayName("invokeBidirectional() should return error response when service throws exception")
    void invokeBidirectionalThrowsException() throws Exception {
      final var methodName = "invokeBidirectionalThrowsException";
      final var method = StubService.class.getMethod(methodName, Flux.class);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeBidirectional(Flux.just(message))
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .assertNext(assertError(500, "Error"))
          .verifyComplete();
    }

    @Test
    @DisplayName("Invocation of method should contain RequestConext")
    void invokeDynamicQualifier() throws Exception {
      final var methodName = "invokeDynamicQualifier";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage("hello/foo123/dynamic/bar456");

      final var authenticator = mock(Authenticator.class);
      when(authenticator.authenticate(ArgumentMatchers.any()))
          .thenReturn(Mono.just(new PrincipalImpl("user", List.of("read", "write"))));

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, authenticator);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .verifyComplete();
    }
  }

  @Nested
  class AuthTests {

    @Test
    @DisplayName("Principal is null, Authenticator is null")
    void testAuthWhenNoPrincipalAndNoAuthenticator() throws Exception {
      final var methodName = "invokeWithAuthContext";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, null);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .assertNext(assertError(401, "Authentication failed"))
          .verifyComplete();
    }

    @Test
    @DisplayName("Principal is not null, Authenticator is null")
    void testAuthWhenThereIsPrincipalAndNoAuthenticator() throws Exception {
      final var methodName = "invokeWithAuthContext";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, authenticator);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, PRINCIPAL)))
          .verifyComplete();
    }

    @Test
    @DisplayName("Principal is null, Authenticator is not null")
    void testAuthNoPrincipalButThereIsAuthenticator() throws Exception {
      final var methodName = "invokeWithAuthContext";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      final var authenticator = mock(Authenticator.class);
      when(authenticator.authenticate(ArgumentMatchers.any()))
          .thenReturn(Mono.just(new PrincipalImpl("admin", List.of("read", "write"))));

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, authenticator);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, NULL_PRINCIPAL)))
          .verifyComplete();
    }

    @ParameterizedTest
    @MethodSource("testAuthWithRoleOrPermissionsSource")
    @DisplayName("Authenticate by principal role or permissions")
    void testAuthWithRoleOrPermissions(Principal principal) throws Exception {
      final var methodName = "invokeWithRoleOrPermissions";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      final var authenticator = mock(Authenticator.class);
      when(authenticator.authenticate(ArgumentMatchers.any())).thenReturn(Mono.just(principal));

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, authenticator);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, principal)))
          .verifyComplete();
    }

    static Stream<Principal> testAuthWithRoleOrPermissionsSource() {
      return Stream.of(
          new PrincipalImpl("invoker", null), new PrincipalImpl(null, List.of("invoke")));
    }

    @ParameterizedTest
    @MethodSource("testAuthFailedWithRoleOrPermissionsMethodSource")
    @DisplayName("Failed to authenticate by principal role or permissions")
    void testAuthFailedWithRoleOrPermissions(Principal principal) throws Exception {
      final var methodName = "invokeWithRoleOrPermissions";
      final var method = StubService.class.getMethod(methodName);
      final var methodInfo = Reflect.methodInfo(StubService.class, method);
      final var message = serviceMessage(methodName);

      final var authenticator = mock(Authenticator.class);
      when(authenticator.authenticate(ArgumentMatchers.any())).thenReturn(Mono.just(principal));

      serviceMethodInvoker = serviceMethodInvoker(method, methodInfo, authenticator);

      StepVerifier.create(
              serviceMethodInvoker
                  .invokeOne(message)
                  .contextWrite(requestContext(message, principal)))
          .assertNext(assertError(403, "Not allowed"))
          .verifyComplete();
    }

    static Stream<Principal> testAuthFailedWithRoleOrPermissionsMethodSource() {
      return Stream.of(
          NULL_PRINCIPAL,
          new PrincipalImpl(null, null),
          new PrincipalImpl("not-invoker", null),
          new PrincipalImpl(null, List.of("not-invoke")));
    }
  }

  private static Context requestContext(ServiceMessage message, Principal principal) {
    return new RequestContext().headers(message.headers()).principal(principal);
  }

  private ServiceMethodInvoker serviceMethodInvoker(
      Method method, MethodInfo methodInfo, Authenticator authenticator) {
    return new ServiceMethodInvoker(
        method,
        stubService,
        methodInfo,
        DefaultErrorMapper.INSTANCE,
        dataDecoder,
        authenticator,
        null);
  }

  private static ServiceMessage serviceMessage(String action) {
    return ServiceMessage.builder()
        .qualifier(Qualifier.asString(StubService.NAMESPACE, action))
        .build();
  }

  private static Consumer<ServiceMessage> assertError(int errorCode, String errorMessage) {
    return message -> {
      assertTrue(message.isError(), "isError");
      final var errorData = (ErrorData) message.data();
      assertEquals(errorCode, errorData.getErrorCode(), "errorCode");
      assertEquals(errorMessage, errorData.getErrorMessage(), "errorMessage");
    };
  }
}
