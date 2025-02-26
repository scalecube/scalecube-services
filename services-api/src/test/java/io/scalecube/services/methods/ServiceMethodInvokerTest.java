package io.scalecube.services.methods;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.RequestContext;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.Method;
import java.util.Collections;
import java.util.Map;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.ArgumentMatchers;
import org.mockito.Mockito;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;
import reactor.test.StepVerifier;

class ServiceMethodInvokerTest {

  private static final boolean IS_SECURED = true;
  public static final boolean IS_RETURN_TYPE_SERVICE_MESSAGE = false;
  public static final boolean IS_REQUEST_TYPE_SERVICE_MESSAGE = false;
  public static final Map<String, String> PRINCIPAL =
      Collections.singletonMap("token", "asdjf9asdjf0as9fkasdf9afkds");

  private final ServiceMessageDataDecoder dataDecoder = (message, type) -> message;
  private final Authenticator authenticator = requestContext -> Mono.just(new Object());
  private final StubService stubService = new StubServiceImpl();

  private ServiceMethodInvoker serviceMethodInvoker;

  @Test
  @DisplayName("invokeOne should return empty response when service returns null")
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
            Collections.emptyList(),
            Collections.emptyList());

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method, stubService, methodInfo, DefaultErrorMapper.INSTANCE, dataDecoder, null, null);

    ServiceMessage message =
        ServiceMessage.builder()
            .qualifier(Qualifier.asString(StubService.NAMESPACE, methodName))
            .build();

    StepVerifier.create(
            Mono.deferContextual(context -> serviceMethodInvoker.invokeOne(message))
                .contextWrite(
                    context ->
                        context.put(
                            RequestContext.class,
                            RequestContext.builder().principal(PRINCIPAL).build())))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeMany should return empty response when service returns null")
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
            Collections.emptyList(),
            Collections.emptyList());

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
            Flux.deferContextual(context -> serviceMethodInvoker.invokeMany(message))
                .contextWrite(
                    context ->
                        context.put(
                            RequestContext.class,
                            RequestContext.builder().principal(PRINCIPAL).build())))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeBidirectional should return empty response when service returns null")
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
            Collections.emptyList(),
            Collections.emptyList());

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
            Flux.deferContextual(
                    context -> serviceMethodInvoker.invokeBidirectional(Flux.just(message)))
                .contextWrite(
                    context ->
                        context.put(
                            RequestContext.class,
                            RequestContext.builder().principal(PRINCIPAL).build())))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeOne should return error response when service throws exception")
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
            Collections.emptyList(),
            Collections.emptyList());

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
    final Mono<ServiceMessage> invokeOne =
        Mono.deferContextual(context -> serviceMethodInvoker.invokeOne(message))
            .contextWrite(
                context ->
                    context.put(
                        RequestContext.class,
                        RequestContext.builder().principal(PRINCIPAL).build()));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeMany should return error response when service throws exception")
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
            Collections.emptyList(),
            Collections.emptyList());

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

    final Flux<ServiceMessage> invokeOne =
        Flux.deferContextual(context -> serviceMethodInvoker.invokeMany(message))
            .contextWrite(
                context ->
                    context.put(
                        RequestContext.class,
                        RequestContext.builder().principal(PRINCIPAL).build()));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeBidirectional should return error response when service throws exception")
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
            Collections.emptyList(),
            Collections.emptyList());

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
    final Flux<ServiceMessage> invokeOne =
        Flux.deferContextual(
                context -> serviceMethodInvoker.invokeBidirectional(Flux.just(message)))
            .contextWrite(
                context ->
                    context.put(
                        RequestContext.class,
                        RequestContext.builder().principal(PRINCIPAL).build()));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "invocation of secured method should return error "
          + "if there're no auth.context and no authenticator")
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
            Collections.emptyList(),
            Collections.emptyList());

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
    final Mono<ServiceMessage> invokeOne = serviceMethodInvoker.invokeOne(message);

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "invocation of secured method should return successfull response "
          + "if auth.context exists and no authenticator")
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
            Collections.emptyList(),
            Collections.emptyList());

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
            Mono.deferContextual(context -> serviceMethodInvoker.invokeOne(message))
                .contextWrite(
                    context ->
                        context.put(
                            RequestContext.class,
                            RequestContext.builder().principal(PRINCIPAL).build())))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "invocation of secured method should return successfull response "
          + "if there're no auth.context but authenticator exists")
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
            Collections.emptyList(),
            Collections.emptyList());

    Authenticator authenticator = Mockito.mock(Authenticator.class);
    Mockito.when(authenticator.authenticate(ArgumentMatchers.any()))
        .thenReturn(Mono.just(new Object()));

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

    StepVerifier.create(serviceMethodInvoker.invokeOne(message)).verifyComplete();
  }

  @Test
  @DisplayName("invocation of secured method should contain RequestConext with all fields")
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
            Collections.emptyList(),
            Collections.emptyList());

    Authenticator authenticator = Mockito.mock(Authenticator.class);
    Mockito.when(authenticator.authenticate(ArgumentMatchers.any()))
        .thenReturn(Mono.just(new Object()));

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

    StepVerifier.create(serviceMethodInvoker.invokeOne(message)).verifyComplete();
  }
}
