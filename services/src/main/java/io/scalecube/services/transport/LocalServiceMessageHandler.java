package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;
import io.scalecube.services.codec.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.BadRequestException;

import org.reactivestreams.Publisher;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.util.Optional;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class LocalServiceMessageHandler implements ServiceMessageHandler {

  private static final String ERROR_DATA_TYPE_MISMATCH = "Expected data of type '%s' but got '%s'";

  private final Method method;
  private final Object service;
  private final Class<?> requestType;
  private final String qualifier;
  private final Class<?> returnType;
  private final ServiceMessageDataCodec dataCodec;
  private final boolean isRequestTypeServiceMessage;
  private boolean isRequestTypeVoid;

  public LocalServiceMessageHandler(String qualifier, Object service, Method method) {
    this.qualifier = qualifier;
    this.service = service;
    this.method = method;
    this.requestType = Reflect.requestType(method);
    this.returnType = Reflect.parameterizedReturnType(method);
    this.dataCodec = new ServiceMessageDataCodec();
    this.isRequestTypeServiceMessage = Reflect.isRequestTypeServiceMessage(method);
    this.isRequestTypeVoid = requestType.isAssignableFrom(Void.TYPE);
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage message) {
    try {
      return Mono.from(invokeMethod(message)).map(this::toResponse);
    } catch (InvocationTargetException ex) {
      return Mono.error(Optional.ofNullable(ex.getCause()).orElse(ex));
    } catch (Throwable ex) {
      return Mono.error(ex);
    }
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message) {
    try {
      return Flux.from(invokeMethod(message)).map(this::toResponse);
    } catch (InvocationTargetException ex) {
      return Flux.error(Optional.ofNullable(ex.getCause()).orElse(ex));
    } catch (Throwable ex) {
      return Flux.error(ex);
    }
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
    return Flux.from(publisher)
        .map(this::toRequest)
        .transform(this::invokeMethod)
        .map(this::toResponse);
  }

  private Object toRequest(ServiceMessage message) {
    ServiceMessage request = dataCodec.decode(message, requestType);
    if (!isRequestTypeVoid && !isRequestTypeServiceMessage && !request.hasData(requestType)) {
      throw new BadRequestException(String.format(ERROR_DATA_TYPE_MISMATCH,
          requestType, Optional.ofNullable(request.data()).map(Object::getClass).orElse(null)));
    }
    return isRequestTypeServiceMessage ? request : request.data();
  }

  private ServiceMessage toResponse(Object response) {
    return (response instanceof ServiceMessage)
        ? (ServiceMessage) response
        : ServiceMessage.builder()
            .qualifier(qualifier)
            .header("_type", returnType.getName())
            .data(response)
            .build();
  }

  private Publisher<?> invokeMethod(ServiceMessage message) throws Exception {
    if (method.getParameterCount() == 0) {
      return (Publisher<?>) method.invoke(service);
    } else {
      return (Publisher<?>) method.invoke(service, toRequest(message));
    }
  }

  private Publisher<?> invokeMethod(Publisher<?> publisher) {
    try {
      return Flux.from((Publisher<?>) method.invoke(service, publisher));
    } catch (InvocationTargetException ex) {
      return Flux.error(Optional.ofNullable(ex.getCause()).orElse(ex));
    } catch (Throwable ex) {
      return Flux.error(ex);
    }
  }
}
