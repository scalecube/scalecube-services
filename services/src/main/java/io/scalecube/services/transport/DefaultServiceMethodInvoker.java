package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMethodInvoke;
import io.scalecube.services.api.ServiceMessage;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class DefaultServiceMethodInvoker implements ServiceMethodInvoke {

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
      return (Flux<T>) Reflect.invoke(serviceObject, method, object);
    } catch (Exception ex) {
      return Flux.error(ex);
    }
  }

  private <T> Mono<T> invokeReturnsMono(ServiceMessage object) {
    try {
      return (Mono<T>) Reflect.invoke(serviceObject, method, object);
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
}
