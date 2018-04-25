package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

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
            return Reflect.invokeMessage(serviceObject, method, message);
          } catch (Exception e) {
            return Mono.just(ExceptionProcessor.toMessage(e));
          }
        }).map(this::toReturnMessage)
        .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
  }
}
