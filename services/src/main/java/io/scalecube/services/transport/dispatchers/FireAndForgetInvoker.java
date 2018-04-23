package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Mono;

public class FireAndForgetInvoker extends AbstractServiceMethodDispatcher<ServiceMessage, Publisher<Void>> {
  
  public FireAndForgetInvoker(Object serviceObject, Method method, ServiceMessageDataCodec payloadCodec) {
    super(serviceObject, method, payloadCodec);
  }

  @Override
  public Publisher<ServiceMessage> invoke(ServiceMessage request) {
    ServiceMessage message = payloadCodec.decodeData(request, super.requestType);
    try {
      return Mono.from(Reflect.invokeMessage(serviceObject, method, message))
          .map(obj->toReturnMessage(obj));
    } catch (Exception e) {
      return Mono.error(e);
    }
  }
}

