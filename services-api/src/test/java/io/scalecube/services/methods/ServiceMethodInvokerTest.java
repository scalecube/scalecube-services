package io.scalecube.services.methods;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.DefaultErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;

import java.lang.reflect.Method;
import java.util.function.Consumer;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class ServiceMethodInvokerTest {

  private static final String qualifierPrefix = "io.scalecube.services.methods.StubService/";
  private static final boolean AUTH = false;
  private static final Authenticator dummyAuthenticator = credentials -> null;

  private final ServiceMessageDataDecoder dataDecoder = (message, type) -> message;
  private final StubService stubService = new StubServiceImpl();

  private ServiceMethodInvoker serviceMethodInvoker;

  private Consumer<Object> requestReleaser =
      obj -> {
        // no-op
      };

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
            false,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            false,
            AUTH);

    // FIXME: 19.02.2020
    serviceMethodInvoker =
        new ServiceMethodInvoker(
            stubService.getClass(),
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            dummyAuthenticator);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(serviceMethodInvoker.invokeOne(message, requestReleaser)).verifyComplete();
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
            false,
            CommunicationMode.REQUEST_STREAM,
            method.getParameterCount(),
            Void.TYPE,
            false,
            AUTH);
    // FIXME: 19.02.2020
    serviceMethodInvoker =
        new ServiceMethodInvoker(
            stubService.getClass(),
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            dummyAuthenticator);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(serviceMethodInvoker.invokeMany(message, requestReleaser)).verifyComplete();
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
            false,
            CommunicationMode.REQUEST_CHANNEL,
            method.getParameterCount(),
            Void.TYPE,
            false,
            AUTH);

    // FIXME: 19.02.2020
    serviceMethodInvoker =
        new ServiceMethodInvoker(
            stubService.getClass(),
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            dummyAuthenticator);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    StepVerifier.create(
            serviceMethodInvoker.invokeBidirectional(Flux.just(message), requestReleaser))
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
            false,
            CommunicationMode.REQUEST_RESPONSE,
            method.getParameterCount(),
            Void.TYPE,
            false,
            AUTH);

    // FIXME: 19.02.2020
    serviceMethodInvoker =
        new ServiceMethodInvoker(
            stubService.getClass(),
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            dummyAuthenticator);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    // invokeOne
    final Mono<ServiceMessage> invokeOne = serviceMethodInvoker.invokeOne(message, requestReleaser);

    StepVerifier.create(invokeOne).assertNext(ServiceMessage::isError).verifyComplete();
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
            false,
            CommunicationMode.REQUEST_STREAM,
            method.getParameterCount(),
            Void.TYPE,
            false,
            AUTH);

    // FIXME: 19.02.2020
    serviceMethodInvoker =
        new ServiceMethodInvoker(
            stubService.getClass(),
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            dummyAuthenticator);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    final Flux<ServiceMessage> invokeOne =
        serviceMethodInvoker.invokeMany(message, requestReleaser);

    StepVerifier.create(invokeOne).assertNext(ServiceMessage::isError).verifyComplete();
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
            false,
            CommunicationMode.REQUEST_CHANNEL,
            method.getParameterCount(),
            Void.TYPE,
            false,
            AUTH);

    // FIXME: 19.02.2020
    serviceMethodInvoker =
        new ServiceMethodInvoker(
            stubService.getClass(),
            method,
            stubService,
            methodInfo,
            DefaultErrorMapper.INSTANCE,
            dataDecoder,
            dummyAuthenticator);

    ServiceMessage message =
        ServiceMessage.builder().qualifier(qualifierPrefix + methodName).build();

    // invokeOne
    final Flux<ServiceMessage> invokeOne =
        serviceMethodInvoker.invokeBidirectional(Flux.just(message), requestReleaser);

    StepVerifier.create(invokeOne).assertNext(ServiceMessage::isError).verifyComplete();
  }
}
