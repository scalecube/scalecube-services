package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RequestStreamDispatcher
    extends AbstractServiceMethodDispatcher<ServiceMessage, Publisher<ServiceMessage>> {

  public RequestStreamDispatcher(String qualifier,
      Object serviceObject,
      Method method,
      ServiceMessageDataCodec payloadCodec) {
    super(qualifier, serviceObject, method, payloadCodec);
  }

  public Publisher<ServiceMessage> invoke(ServiceMessage request) {
    ServiceMessage message = payloadCodec.decodeData(request, super.requestType);
    try {
      return Flux.from(super.dispatchServiceMethod(message)).map(this::toReturnMessage);
    } catch (Exception e) {
      return Mono.error(e);
    }
  }
}
