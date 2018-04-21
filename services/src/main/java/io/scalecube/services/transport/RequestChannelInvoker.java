package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;

public class RequestChannelInvoker extends AbstractServiceMethodInvoker<Publisher<ServiceMessage>, Publisher<ServiceMessage>> {

  public RequestChannelInvoker(Object serviceObject, Method method,
      ServiceMessageCodec<?> payloadCodec) {
    
    super(serviceObject, method, payloadCodec);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> request) {
    try {
      return Flux.from(Reflect.invokeMessage(serviceObject, method, request))
          .map(object -> toMessage(object))
          .map(resp -> payloadCodec.encodeData(resp));
    } catch (Exception error) {
     return Flux.error(error);
    }
   
  }
}

