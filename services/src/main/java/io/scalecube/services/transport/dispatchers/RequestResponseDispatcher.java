package io.scalecube.services.transport.dispatchers;

import static reactor.core.publisher.Mono.just;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;
import java.util.function.Function;

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
    ServiceMessage message = payloadCodec.decodeData(request, super.requestType);
    try {
      Mono<ServiceMessage> map =
          Mono.from(Reflect.invokeMessage(serviceObject, method, message)).map(this::toReturnMessage);
      return map.onErrorResume(toServiceMsg());
    } catch (Throwable e) {
      return Mono.error(e);
    }
  }

  private Function<Throwable, Mono<? extends ServiceMessage>> toServiceMsg() {
    return t -> just(ServiceMessage.builder()
        .qualifier("ERROR_FATAL")
        .data(new ErrData().setErrCode(500).setErrMessage("Error Fatal occurred man..."))
        .build());
  }
}

