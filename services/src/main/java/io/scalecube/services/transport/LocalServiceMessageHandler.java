package io.scalecube.services.transport;

import io.scalecube.services.CommunicationMode;
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
  private final ServiceMessageDataCodec dataCodec;
  private final CommunicationMode mode;

  public LocalServiceMessageHandler(String qualifier, Object service, Method method) {
    this.qualifier = qualifier;
    this.service = service;
    this.method = method;
    this.requestType = Reflect.requestType(method);
    this.returnType = Reflect.parameterizedReturnType(method);
    this.dataCodec = new ServiceMessageDataCodec();
    this.mode = Reflect.communicationMode(method);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    switch (mode) {
      case FIRE_AND_FORGET:
        return Mono.from(publisher)
            .flatMap(request -> Mono.from(invokeOrThrow(request)))
            .map(response -> null);
      case REQUEST_RESPONSE:
        return Mono.from(publisher)
            .flatMap(request -> Mono.from(invokeOrThrow(request)))
            .map(this::toResponse);
      case REQUEST_STREAM:
        return Flux.from(publisher)
            .flatMap(request -> Flux.from(invokeOrThrow(request)))
            .map(this::toResponse);
      case REQUEST_CHANNEL:
        // falls to default
      default:
        throw new IllegalArgumentException("Communication mode is not supported: " + method);
    }
  }

  private Publisher<?> invokeOrThrow(ServiceMessage request) {
    return Reflect.invokeOrThrow(service, method, dataCodec.decode(request, requestType));
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
}
