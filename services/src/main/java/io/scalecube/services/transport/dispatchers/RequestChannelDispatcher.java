package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

public class RequestChannelDispatcher
    extends AbstractServiceMethodDispatcher<Publisher<ServiceMessage>, Publisher<ServiceMessage>> {

  public RequestChannelDispatcher(String qualifier, Object serviceObject, Method method) {
    super(qualifier, serviceObject, method);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    // FIXME: need to seek handler and invoke it.
    throw new UnsupportedOperationException();
  }
}
