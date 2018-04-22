package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestChannelInvoker
    extends AbstractServiceMethodInvoker<Publisher<ServiceMessage>, Publisher<ServiceMessage>> {

  public RequestChannelInvoker(Object serviceObject, Method method,
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

