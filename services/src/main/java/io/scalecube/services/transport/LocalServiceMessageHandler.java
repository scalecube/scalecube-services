package io.scalecube.services.transport;

import io.scalecube.services.CommunicationMode;
import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.api.ServiceMessageHandler;
import io.scalecube.services.codec.ServiceMessageDataCodec;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public final class LocalServiceMessageHandler implements ServiceMessageHandler {

  private final Method method;
  private final Object service;
  private final Class<?> requestType;
  private final String qualifier;
  private final Class<?> returnType;
  private final ServiceMessageDataCodec dataCodec;
  private final CommunicationMode mode;
  private final boolean isRequestTypeServiceMessage;

  public LocalServiceMessageHandler(String qualifier, Object service, Method method) {
    this.qualifier = qualifier;
    this.service = service;
    this.method = method;
    this.requestType = Reflect.requestType(method);
    this.returnType = Reflect.parameterizedReturnType(method);
    this.dataCodec = new ServiceMessageDataCodec();
    this.mode = Reflect.communicationMode(method);
    this.isRequestTypeServiceMessage = Reflect.isRequestTypeServiceMessage(method);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    return Flux.from(publisher)
        .map(this::toRequest)
        .transform((Flux<?> publisher1) -> Reflect.invokePublisher(service, method, mode, publisher1))
        .map(this::toResponse);
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

  private Object toRequest(ServiceMessage message) {
    ServiceMessage request = dataCodec.decode(message, requestType);
    return isRequestTypeServiceMessage ? request : request.data();
  }
}
