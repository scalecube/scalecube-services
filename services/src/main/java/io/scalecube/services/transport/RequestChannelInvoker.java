package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RequestChannelInvoker extends AbstractServiceMethodInvoker<Publisher<ServiceMessage>, Publisher<ServiceMessage>> {


  public RequestChannelInvoker(Object serviceObject, Method method,
      Class<?> reqType, 
      Class<?> respType,
      ServiceMessageCodec<?> payloadCodec) {
    
    super(serviceObject, method, reqType, respType, payloadCodec);
  }

  @Override
  public Publisher<ServiceMessage> invoke(Publisher<ServiceMessage> request) {
    return Flux.from(request)
    .map(req -> {
      try {
        return Reflect.invokeMessage(serviceObject, method, req);
      } catch (Exception e) {
        return Flux.error(e);
      }
    }).map(object -> toMessage(object))
    .map(resp -> payloadCodec.encodeData(resp));
   
  }
}

