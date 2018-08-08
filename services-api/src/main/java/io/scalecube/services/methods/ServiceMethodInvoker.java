package io.scalecube.services.methods;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.BadRequestException;

import org.reactivestreams.Publisher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;
import java.util.function.BiFunction;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class ServiceMethodInvoker {

  private final Method method;
  private final Object service;
  private final MethodInfo methodInfo;

  public ServiceMethodInvoker(Method method, Object service, MethodInfo methodInfo) {
    this.method = method;
    this.service = service;
    this.methodInfo = methodInfo;
  }

  public Mono<ServiceMessage> invokeOne(ServiceMessage message,
      BiFunction<ServiceMessage, Class<?>, ServiceMessage> dataDecoder) {
    return Mono.from(invoke(toRequest(message, dataDecoder))).map(this::toResponse);
  }

  public Flux<ServiceMessage> invokeMany(ServiceMessage message,
      BiFunction<ServiceMessage, Class<?>, ServiceMessage> dataDecoder) {
    return Flux.from(invoke(toRequest(message, dataDecoder))).map(this::toResponse);
  }

  public Flux<ServiceMessage> invokeBidirectional(Publisher<ServiceMessage> publisher,
      BiFunction<ServiceMessage, Class<?>, ServiceMessage> dataDecoder) {
    return Flux.from(invoke(Flux.from(publisher).map(message -> toRequest(message, dataDecoder))))
        .map(this::toResponse);
  }

  private Publisher<?> invoke(Object arguments) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      if (method.getParameterCount() == 0) {
        result = (Publisher<?>) method.invoke(service);
      } else {
        result = (Publisher<?>) method.invoke(service, arguments);
      }
      if (result == null) {
        result = Mono.empty();
      }
    } catch (InvocationTargetException ex) {
      throwable = Optional.ofNullable(ex.getCause()).orElse(ex);
    } catch (Throwable ex) {
      throwable = ex;
    }
    return throwable != null ? Mono.error(throwable) : result;
  }

  private Object toRequest(ServiceMessage message,
      BiFunction<ServiceMessage, Class<?>, ServiceMessage> dataDecoder) {
    ServiceMessage request = dataDecoder.apply(message, methodInfo.requestType());

    if (!methodInfo.isRequestTypeVoid() &&
        !methodInfo.isRequestTypeServiceMessage() &&
        !request.hasData(methodInfo.requestType())) {

      Class<?> aClass = Optional.ofNullable(request.data()).map(Object::getClass).orElse(null);
      throw new BadRequestException(String.format("Expected service request data of type: %s, but received: %s",
          methodInfo.requestType(), aClass));
    }

    return methodInfo.isRequestTypeServiceMessage() ? request : request.data();
  }

  private ServiceMessage toResponse(Object response) {
    return (response instanceof ServiceMessage)
        ? (ServiceMessage) response
        : ServiceMessage.builder().qualifier(methodInfo.qualifier()).data(response).build();
  }

}
