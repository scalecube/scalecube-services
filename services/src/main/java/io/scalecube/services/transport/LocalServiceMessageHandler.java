package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;
import io.scalecube.services.codec.ServiceMessageDataCodec;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public final class LocalServiceMessageHandler implements ServiceMessageHandler {

  private final Method method;
  private final Object service;
  private final Class<?> requestType;
  private final String qualifier;
  private final Class<?> returnType;
  private final Class<?> parameterizedReturnType;
  private final ServiceMessageDataCodec dataCodec;

  public LocalServiceMessageHandler(String qualifier, Object service, Method method) {
    this.qualifier = qualifier;
    this.service = service;
    this.method = method;
    this.requestType = Reflect.requestType(method);
    this.returnType = method.getReturnType();
    this.parameterizedReturnType = Reflect.parameterizedReturnType(method);
    this.dataCodec = new ServiceMessageDataCodec();
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    if (returnType.isAssignableFrom(Mono.class) && parameterizedReturnType.isAssignableFrom(Void.class)) {
      // FireAndForget
      return Mono.from(publisher)
          .flatMap(request -> Mono.from(invokeOrThrow(request)));
    }
    if (returnType.isAssignableFrom(Mono.class)) {
      // RequestResponse
      return Mono.from(publisher)
          .flatMap(request -> Mono.from(invokeOrThrow(request)))
          .map(this::toResponse);
    }
    if (returnType.isAssignableFrom(Flux.class) && requestType.isAssignableFrom(Flux.class)) {
      // RequestChannel
      throw new UnsupportedOperationException("No supported service signature: srv(Flux) -> Flux");
    }
    if (returnType.isAssignableFrom(Flux.class)) {
      // RequestStream
      return Mono.from(publisher)
          .transform(mono -> mono.map(request -> Flux.from(invokeOrThrow(request))))
          .map(this::toResponse);
    } else {
      throw new IllegalArgumentException(
          "Service method is not supported (check return type or parameter type): " + method);
    }
  }

  private Publisher<ServiceMessage> invokeOrThrow(ServiceMessage request) {
    return Reflect.invokeOrThrow(service, method, dataCodec.decode(request, requestType));
  }

  private ServiceMessage toResponse(Object response) {
    return dataCodec.encode(
        (response instanceof ServiceMessage)
            ? (ServiceMessage) response
            : ServiceMessage
                .builder()
                .qualifier(qualifier)
                .header("_type", returnType.getName())
                .data(response)
                .build());
  }
}
