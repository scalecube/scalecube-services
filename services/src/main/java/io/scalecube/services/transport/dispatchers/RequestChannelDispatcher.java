package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestChannelDispatcher
    extends AbstractServiceMethodDispatcher<Flux<ServiceMessage>, Flux<ServiceMessage>> {

  public RequestChannelDispatcher(String qualifier, Object serviceObject, Method method) {
    super(qualifier, serviceObject, method);
  }

  @Override
  public Flux<ServiceMessage> invoke(Flux<ServiceMessage> publisher) {
    // FIXME: need to seek handler and invoke it.
    throw new UnsupportedOperationException();
  }
}
