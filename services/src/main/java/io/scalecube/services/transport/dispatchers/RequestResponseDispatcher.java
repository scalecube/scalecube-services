package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import java.lang.reflect.Method;

import reactor.core.publisher.Mono;

public class RequestResponseDispatcher extends AbstractServiceMethodDispatcher {

  public RequestResponseDispatcher(String qualifier, Object serviceObject, Method method) {
    super(qualifier, serviceObject, method);
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    try {
      return Mono.from(Reflect.invoke(serviceObject, method, request)).map(this::toReturnMessage);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }
}
