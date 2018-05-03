package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestChannelDispatcher extends AbstractServiceMethodDispatcher {

  public RequestChannelDispatcher(String qualifier, Object serviceObject, Method method) {
    super(qualifier, serviceObject, method);
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    // FIXME: need to seek handler and invoke it.
    throw new UnsupportedOperationException("requestChannel is not implemented");
  }
}
