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
import java.util.function.Function;

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
    return Mono.from(invokeMethod(message, Mono::error))
        .map(this::toResponse)
        .switchIfEmpty(Mono.just(toEmptyResponse()));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage message) {
    return Flux.from(invokeMethod(message, Flux::error))
        .map(this::toResponse)
        .switchIfEmpty(Flux.just(toEmptyResponse()));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Publisher<ServiceMessage> publisher) {
    return Flux.from(invokeMethod(Flux.from(publisher).map(this::toRequest), Flux::error))
        .map(this::toResponse)
        .switchIfEmpty(Flux.just(toEmptyResponse()));
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

  public ServiceMessage toEmptyResponse() {
    return ServiceMessage.builder()
        .qualifier(qualifier)
        .header("_type", returnType.getName())
        .build();
  }

  private Publisher<?> invokeMethod(ServiceMessage message,
      Function<Throwable, Publisher<?>> exceptionMapper) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      if (method.getParameterCount() == 0) {
        result = (Publisher<?>) method.invoke(service);
      } else {
        result = (Publisher<?>) method.invoke(service, toRequest(message));
      }
    } catch (InvocationTargetException ex) {
      throwable = Optional.ofNullable(ex.getCause()).orElse(ex);
    } catch (Throwable ex) {
      throwable = ex;
    }
    return throwable != null ? exceptionMapper.apply(throwable) : result;
  }

  private Publisher<?> invokeMethod(Publisher<?> publisher,
      Function<Throwable, Publisher<?>> exceptionMapper) {
    Publisher<?> result = null;
    Throwable throwable = null;
    try {
      result = (Publisher<?>) method.invoke(service, publisher);
    } catch (InvocationTargetException ex) {
      throwable = Optional.ofNullable(ex.getCause()).orElse(ex);
    } catch (Throwable ex) {
      throwable = ex;
    }
    return throwable != null ? exceptionMapper.apply(throwable) : result;
  }
}
