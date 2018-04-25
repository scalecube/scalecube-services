package io.scalecube.services.transport.dispatchers;

import static io.scalecube.services.Reflect.invokeMessage;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.exceptions.ExceptionProcessor;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Mono;

public class RequestResponseDispatcher
    extends AbstractServiceMethodDispatcher<ServiceMessage, Publisher<ServiceMessage>> {

  public RequestResponseDispatcher(String qualifier,
      Object serviceObject,
      Method method,
      ServiceMessageDataCodec payloadCodec) {
    super(qualifier, serviceObject, method, payloadCodec);
  }

  @Override
  public Publisher<ServiceMessage> invoke(ServiceMessage request) {
    try {
      ServiceMessage message = payloadCodec.decodeData(request, requestType);
      return Mono.from(invokeMessage(serviceObject, method, message))
          .map(this::toReturnMessage)
          .onErrorResume(t -> Mono.just(ExceptionProcessor.toMessage(t)));
    } catch (Throwable e) {
      return Mono.just(ExceptionProcessor.toMessage(e));
    }
  }
}
