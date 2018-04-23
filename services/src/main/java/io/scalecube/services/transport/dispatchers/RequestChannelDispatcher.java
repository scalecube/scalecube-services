package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestChannelDispatcher
    extends AbstractServiceMethodDispatcher<Publisher<ServiceMessage>, Publisher<ServiceMessage>> {

  public RequestChannelDispatcher(Object serviceObject, Method method,
      ServiceMessageDataCodec payloadCodec) {

    super(serviceObject, method, payloadCodec);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> request) {
    try {
      return Flux.from(Reflect.invokeMessage(serviceObject, method, request))
          .map(object -> toReturnMessage(object));
    } catch (Exception error) {
      return Flux.error(error);
    }

  }
}

