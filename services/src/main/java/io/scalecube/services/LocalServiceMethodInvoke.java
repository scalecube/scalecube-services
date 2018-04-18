package io.scalecube.services;

import io.scalecube.services.api.ServiceMessage;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class LocalServiceMethodInvoke implements ServiceMethodInvoke {

  Method method;
  Object serviceObject;
  Type requestType = Reflect.parameterizedType(method);
  Type returnType = Reflect.parameterizedReturnType(method);

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    
    Object object = toObject(decodeData(request, requestType));
    return invokeReturnsMono(object)
        .map(val -> toMessage(val))
        .map(resp -> encodeData(resp));
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    Object object = toObject(decodeData(request, requestType));
    invokeReturnsMono(object);
    return Mono.empty();
  }


  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return request.map(req -> decodeData(req, requestType))
        .map(message -> toObject(message))
        .map(object -> invokeReturnsFlux(object))
        .map(object -> toMessage(object))
        .map(resp -> encodeData(resp));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    Object object = toObject(decodeData(request, requestType));
    return invokeReturnsFlux(object)
        .map(val -> toMessage(val))
        .map(resp -> encodeData(resp));
  }

  private <T> Flux<T> invokeReturnsFlux(Object object) {
    try {
      return (Flux<T>) method.invoke(serviceObject, object);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  private <T> Mono<T> invokeReturnsMono(Object object) {
    try {
      return (Mono<T>) method.invoke(serviceObject, object);
    } catch (Exception ex) {
      ex.printStackTrace();
    }
    return null;
  }

  private ServiceMessage decodeData(ServiceMessage msg, Type requestType) {
    // TODO Auto-generated method stub
    return null;
  }

  private ServiceMessage encodeData(ServiceMessage resp) {
    // TODO Auto-generated method stub
    return null;
  }

  private ServiceMessage toMessage(Object object) {
    if (requestType.equals(ServiceMessage.class)) {
      return (ServiceMessage) object;
    } else {
      return ServiceMessage.builder().data(object).build();
    }
  }

  private Object toObject(ServiceMessage message) {
    if (returnType.equals(ServiceMessage.class)) {
      return message;
    } else {
      return message.data();
    }
  }
}
