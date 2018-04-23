package io.scalecube.services.transport.dispatchers;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.AbstractServiceMethodDispatcher;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RequestStreamDispatcher extends AbstractServiceMethodDispatcher<ServiceMessage, Publisher<ServiceMessage>> {

  public RequestStreamDispatcher(Object serviceObject, Method method, 
      ServiceMessageDataCodec payloadCodec) {
    
    super(serviceObject, method, payloadCodec);
  }

  public Publisher<ServiceMessage> invoke(ServiceMessage request) {
    
    ServiceMessage message = payloadCodec.decodeData(request, super.requestType);
    try {
      return Flux.from(Reflect.invokeMessage(serviceObject, method, message))
          .map(obj->toReturnMessage(obj));
      
    } catch (Exception e) {
      return Mono.error(e);
    }
    
  }

}
