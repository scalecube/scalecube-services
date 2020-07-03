package io.scalecube.services.methods;

import static io.scalecube.services.auth.Authenticator.AUTH_CONTEXT_KEY;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.auth.PrincipalMapper;
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
import reactor.test.StepVerifier;

class ServiceMethodInvokerTest {

  private static final String qualifierPrefix = "io.scalecube.services.methods.StubService/";

  private static final boolean AUTH = true;
  public static final boolean IS_RETURN_TYPE_SERVICE_MESSAGE = false;
  public static final boolean IS_REQUEST_TYPE_SERVICE_MESSAGE = false;
  public static final Map<String, String> AUTH_DATA =
      Collections.singletonMap("token", "asdjf9asdjf0as9fkasdf9afkds");

  private final ServiceMessageDataDecoder dataDecoder = (message, type) -> message;
  private final PrincipalMapper<Object, Object> principalMapper = authData -> authData;
  private final StubService stubService = new StubServiceImpl();

  private ServiceMethodInvoker serviceMethodInvoker;

  @Test
  @DisplayName("invokeOne should return empty response when service returns null")
  void testInvokeOneWhenReturnNull() throws Exception {
    final String methodName = "returnNull";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(
            Mono.deferWithContext(context -> serviceMethodInvoker.invokeOne(message))
                .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA)))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeMany should return empty response when service returns null")
  void testInvokeManyWhenReturnNull() throws Exception {
    final String methodName = "returnNull2";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_STREAM,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(
            Flux.deferWithContext(context -> serviceMethodInvoker.invokeMany(message))
                .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA)))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeBidirectional should return empty response when service returns null")
  void testInvokeBidirectionalWhenReturnNull() throws Exception {
    final String methodName = "returnNull3";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName, Flux.class);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_CHANNEL,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(
            Flux.deferWithContext(
                    context -> serviceMethodInvoker.invokeBidirectional(Flux.just(message)))
                .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA)))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeOne should return error response when service throws exception")
  void testInvokeOneWhenThrowException() throws Exception {
    final String methodName = "throwException";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    // invokeOne
    final Mono<ServiceMessage> invokeOne =
        Mono.deferWithContext(context -> serviceMethodInvoker.invokeOne(message))
            .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeMany should return error response when service throws exception")
  void testInvokeManyWhenThrowException() throws Exception {
    final String methodName = "throwException2";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_STREAM,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    final Flux<ServiceMessage> invokeOne =
        Flux.deferWithContext(context -> serviceMethodInvoker.invokeMany(message))
            .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName("invokeBidirectional should return error response when service throws exception")
  void testInvokeBidirectionalWhenThrowException() throws Exception {
    final String methodName = "throwException3";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName, Flux.class);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_CHANNEL,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    // invokeOne
    final Flux<ServiceMessage> invokeOne =
        Flux.deferWithContext(
                context -> serviceMethodInvoker.invokeBidirectional(Flux.just(message)))
            .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "invocation of auth method should return error "
          + "if there're no auth.context and no authenticator")
  void testAuthMethodWhenNoContextAndNoAuthenticator() throws Exception {
    final String methodName = "throwException";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    // invokeOne
    final Mono<ServiceMessage> invokeOne =
        Mono.deferWithContext(context -> serviceMethodInvoker.invokeOne(message))
            .subscriberContext(context -> context.put(AUTH_CONTEXT_KEY, AUTH_DATA));

    StepVerifier.create(invokeOne)
        .assertNext(serviceMessage -> Assertions.assertTrue(serviceMessage.isError()))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "invocation of auth method should return empty response "
          + "if auth.context exists and no authenticator")
  void testAuthMethodWhenThereIsContextAndNoAuthenticator() throws Exception {
    final String methodName = "helloAuthContext";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(
            Mono.deferWithContext(context -> serviceMethodInvoker.invokeOne(message))
                .subscriberContext(
                    context ->
                        context.put(AUTH_CONTEXT_KEY, AUTH_DATA).put("NON_AUTH_CONTEXT", "test")))
        .verifyComplete();
  }

  @Test
  @DisplayName(
      "invocation of auth method should return empty response "
          + "if there're no auth.context but authenticator exists")
  void testAuthMethodWhenNoContextButThereIsAuthenticator() throws Exception {
    final String methodName = "helloAuthContext";
    final Class<? extends StubService> serviceClass = stubService.getClass();
    final Method method = serviceClass.getMethod(methodName);

    final MethodInfo methodInfo =
        new MethodInfo(
            serviceClass.getName(),
            methodName,
            method.getReturnType(),
            IS_RETURN_TYPE_SERVICE_MESSAGE,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            IS_REQUEST_TYPE_SERVICE_MESSAGE,
            AUTH);

    //noinspection unchecked
    Authenticator<Object> mockedAuthenticator = Mockito.mock(Authenticator.class);
    Mockito.when(mockedAuthenticator.authenticate(ArgumentMatchers.anyMap()))
        .thenReturn(Mono.just(AUTH_DATA));

    serviceMethodInvoker =
        new ServiceMethodInvoker(
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            principalMapper);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(
            Mono.deferWithContext(context -> serviceMethodInvoker.invokeOne(message))
                .subscriberContext(
                    context ->
                        context.put(AUTH_CONTEXT_KEY, AUTH_DATA).put("NON_AUTH_CONTEXT", "test")))
        .verifyComplete();
  }
}
