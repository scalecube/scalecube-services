package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestStreamDispatcher
    extends AbstractServiceMethodDispatcher<ServiceMessage, Publisher<ServiceMessage>> {

  public RequestStreamDispatcher(String qualifier, Object serviceObject, Method method) {
    super(qualifier, serviceObject, method);
  }

  public Flux<ServiceMessage> invoke(ServiceMessage request) {
    try {
      return Flux.from(Reflect.invoke(serviceObject, method, request)).map(this::toReturnMessage);
    } catch (Exception e) {
      return Flux.error(e);
    }
  }
}
