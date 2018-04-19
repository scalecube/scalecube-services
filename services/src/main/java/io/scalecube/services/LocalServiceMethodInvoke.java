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
    return invokeReturnsMono(decodeData(request, requestType))
        .map(object -> toMessage(object))
        .map(resp -> encodeData(resp));
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return invokeReturnsMono(decodeData(request, requestType));
  }


  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return request.map(req -> decodeData(req, requestType))
        .map(message -> invokeReturnsFlux(message))
        .map(object -> toMessage(object))
        .map(resp -> encodeData(resp));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return invokeReturnsFlux(decodeData(request, requestType))
        .map(object -> toMessage(object))
        .map(resp -> encodeData(resp));
  }

  private <T> Flux<T> invokeReturnsFlux(ServiceMessage object) {
    try {
      return (Flux<T>) invoke(serviceObject, method, object);
    } catch (Exception ex) {
      return Flux.error(ex);
    }
  }

  private <T> Mono<T> invokeReturnsMono(ServiceMessage object) {
    try {
      return (Mono<T>) invoke(serviceObject, method, object);
    } catch (Exception ex) {
      return Mono.error(ex);
    }
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

  /**
   * invoke a java method by a given ServiceMessage.
   *
   * @param serviceObject instance to invoke its method.
   * @param method method to invoke.
   * @param request stream message request containing data or message to invoke.
   * @return invoke result.
   * @throws Exception in case method expects more then one parameter
   */
  @SuppressWarnings("unchecked")
  public static <T> T invoke(Object serviceObject, Method method, final ServiceMessage request) throws Exception {
    // handle invoke
    if (method.getParameters().length == 0) { // method expect no params.
      return (T) method.invoke(serviceObject);
    } else if (method.getParameters().length == 1) { // method expect 1 param.
      if (method.getParameters()[0].getType().isAssignableFrom(ServiceMessage.class)) {
        return (T) method.invoke(serviceObject, request);
      } else {
        return (T) method.invoke(serviceObject, new Object[] {request.data()});
      }
    } else {
      // should we later support 2 parameters? message and the Stream processor?
      throw new UnsupportedOperationException("Service Method can accept 0 or 1 paramters only!");
    }
  }
}
