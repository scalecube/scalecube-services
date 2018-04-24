package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestChannelDispatcher
    extends AbstractServiceMethodDispatcher<Publisher<ServiceMessage>, Publisher<ServiceMessage>> {

  public RequestChannelDispatcher(String qualifier,
      Object serviceObject,
      Method method,
      ServiceMessageDataCodec payloadCodec) {
    super(qualifier, serviceObject, method, payloadCodec);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> publisher) {
    return Flux.from(publisher).map(request -> payloadCodec.decodeData(request, super.requestType))
        .map(message -> {
          try {
            return super.dispatchServiceMethod(message);
          } catch (Exception e) {
            return Flux.error(e);
          }
        }).map(this::toReturnMessage);
  }
}
