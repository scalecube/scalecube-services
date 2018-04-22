package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;

import org.reactivestreams.Publisher;

import java.lang.reflect.Method;

import reactor.core.publisher.Mono;

public class RequestResponseInvoker extends AbstractServiceMethodInvoker<ServiceMessage, Publisher<ServiceMessage>> {


  public RequestResponseInvoker(Object serviceObject, Method method, 
      ServiceMessageDataCodec payloadCodec) {
    
    super(serviceObject, method, payloadCodec);
  }

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

