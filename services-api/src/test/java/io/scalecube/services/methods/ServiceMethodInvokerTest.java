package io.scalecube.services.methods;

import static io.scalecube.services.auth.Principal.NULL_PRINCIPAL;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.Principal;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.exceptions.UnauthorizedException;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.Method;
import java.util.List;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;
import reactor.util.context.Context;

class ServiceMethodInvokerTest {

  private static final boolean IS_SECURED = true;
  private static final boolean IS_RETURN_TYPE_SERVICE_MESSAGE = false;
  private static final boolean IS_REQUEST_TYPE_SERVICE_MESSAGE = false;
  private static final PrincipalImpl PRINCIPAL = new PrincipalImpl("alice", List.of("permission"));

  private final ServiceMessageDataDecoder dataDecoder = (message, type) -> message;
  private final Authenticator authenticator = requestContext -> Mono.just(PRINCIPAL);
  private final StubService stubService = new StubServiceImpl();

  private ServiceMethodInvoker serviceMethodInvoker;

  @Test
  @DisplayName("invokeOne() should return empty response when service returns null")
  void testInvokeOneWhenReturnNull() throws Exception {
    final String methodName = "returnNull";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method, stubService, methodInfo, DefaultErrorMapper.INSTANCE, dataDecoder, null, null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    StepVerifier.create(
            serviceMethodInvoker
                .invokeOne(message)
                .contextWrite(requestContext(message, PRINCIPAL)))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeMany() should return empty response when service returns null")
  void testInvokeManyWhenReturnNull() throws Exception {
    final String methodName = "returnNull2";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_STREAM,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    StepVerifier.create(
            serviceMethodInvoker
                .invokeMany(message)
                .contextWrite(requestContext(message, PRINCIPAL)))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeBidirectional() should return empty response when service returns null")
  void testInvokeBidirectionalWhenReturnNull() throws Exception {
    final String methodName = "returnNull3";
    final Method method = stubService.getClass().getMethod(methodName, Flux.class);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_CHANNEL,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    StepVerifier.create(
            serviceMethodInvoker
                .invokeBidirectional(Flux.just(message))
                .contextWrite(requestContext(message, PRINCIPAL)))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeOne() should return error response when service throws exception")
  void testInvokeOneWhenThrowException() throws Exception {
    final String methodName = "throwException";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    // invokeOne

    StepVerifier.create(
            serviceMethodInvoker
                .invokeOne(message)
                .contextWrite(requestContext(message, PRINCIPAL)))
        .assertNext(
            serviceMessage -> {
              assertTrue(serviceMessage.isError());
              assertEquals(500, serviceMessage.errorType());
            })
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeMany() should return error response when service throws exception")
  void testInvokeManyWhenThrowException() throws Exception {
    final String methodName = "throwException2";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_STREAM,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    StepVerifier.create(
            serviceMethodInvoker
                .invokeMany(message)
                .contextWrite(requestContext(message, PRINCIPAL)))
        .assertNext(
            serviceMessage -> {
              assertTrue(serviceMessage.isError());
              assertEquals(500, serviceMessage.errorType());
            })
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeBidirectional() should return error response when service throws exception")
  void testInvokeBidirectionalWhenThrowException() throws Exception {
    final String methodName = "throwException3";
    final Method method = stubService.getClass().getMethod(methodName, Flux.class);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_CHANNEL,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    // invokeOne

    StepVerifier.create(
            serviceMethodInvoker
                .invokeBidirectional(Flux.just(message))
                .contextWrite(requestContext(message, PRINCIPAL)))
        .assertNext(
            serviceMessage -> {
              assertTrue(serviceMessage.isError());
              assertEquals(500, serviceMessage.errorType());
            })
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "Invocation of secured method should return error - "
          + "if there is no principal and no authenticator")
  void testAuthMethodWhenNoContextAndNoAuthenticator() throws Exception {
    final String methodName = "helloAuthContext";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method, stubService, methodInfo, DefaultErrorMapper.INSTANCE, dataDecoder, null, null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    // invokeOne

    StepVerifier.create(
            serviceMethodInvoker
                .invokeOne(message)
                .contextWrite(requestContext(message, NULL_PRINCIPAL)))
        .verifyErrorSatisfies(
            ex -> {
              assertInstanceOf(UnauthorizedException.class, ex);
              assertEquals(401, ((UnauthorizedException) ex).errorCode());
            });
  }

  @Test
  @DisplayName(
      "Invocation of secured method should return successfull response "
          + "if there is principal and no authenticator")
  void testAuthMethodWhenThereIsContextAndNoAuthenticator() throws Exception {
    final String methodName = "helloAuthContext";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    StepVerifier.create(
            serviceMethodInvoker
                .invokeOne(message)
                .contextWrite(requestContext(message, PRINCIPAL)))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "Invocation of secured method should return successfull response "
          + "if there is no principal but authenticator exists")
  void testAuthMethodWhenNoContextButThereIsAuthenticator() throws Exception {
    final String methodName = "helloAuthContext";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    Authenticator authenticator = Mockito.mock(Authenticator.class);
    Mockito.when(authenticator.authenticate(ArgumentMatchers.any()))
        .thenReturn(Mono.just(new PrincipalImpl("bob", List.of("read", "write"))));

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    // invokeOne

    StepVerifier.create(
            serviceMethodInvoker
                .invokeOne(message)
                .contextWrite(requestContext(message, NULL_PRINCIPAL)))
        .verifyComplete();
  }

  @Test
  @DisplayName("Invocation of secured method should contain RequestConext")
  void testRequestContextWithDynamicQualifier() throws Exception {
    final String methodName = "helloRequestContextWithDynamicQualifier";
    final String actionName = "hello/:foo/dynamic/:bar";
    final String actualActionName = "hello/foo123/dynamic/bar456";
    final Method method = stubService.getClass().getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            StubService.NAMESPACE,
            actionName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            IS_SECURED,
            Schedulers.immediate(),
            null,
            null,
            null);

    Authenticator authenticator = Mockito.mock(Authenticator.class);
    Mockito.when(authenticator.authenticate(ArgumentMatchers.any()))
        .thenReturn(Mono.just(new PrincipalImpl("bob", List.of("read", "write"))));

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            authenticator,
            null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, actualActionName))
            .build();

    StepVerifier.create(
            serviceMethodInvoker
                .invokeOne(message)
                .contextWrite(requestContext(message, PRINCIPAL)))
        .verifyComplete();
  }

  private static Context requestContext(ServiceMessage message, Principal principal) {
    return RequestContext.builder()
        .headers(message.headers())
        .principal(principal)
        .build()
        .toContext();
  }

  record PrincipalImpl(String serviceRole, List<String> permissions) implements Principal {

    @Override
    public boolean hasPermission(String permission) {
      return permissions.contains(permission);
    }
  }
}
