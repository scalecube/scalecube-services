package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;

import java.lang.reflect.Method;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public abstract class AbstractServiceMethodDispatcher implements ServiceMethodDispatcher {

  protected final Method method;
  protected final Object serviceObject;
  protected final Class requestType;
  protected final String methodName;
  protected final String qualifier;
  protected final Class returnType;

  public AbstractServiceMethodDispatcher(String qualifier, Object serviceObject, Method method) {
    this.qualifier = qualifier;
    this.serviceObject = serviceObject;
    this.method = method;
    this.methodName = Reflect.methodName(method);
    this.requestType = Reflect.requestType(method);
    this.returnType = Reflect.parameterizedReturnType(method);
  }

  protected ServiceMessage toReturnMessage(Object obj) {
    return obj instanceof ServiceMessage
        ? (ServiceMessage) obj
        : ServiceMessage.builder().qualifier(qualifier).header("_type", returnType.getName()).data(obj).build();
  }

  @Override
  public Class requestType() {
    return requestType;
  }

  @Override
  public Class returnType() {
    return returnType;
  }

  @Override
  public Mono<ServiceMessage> requestResponse(ServiceMessage request) {
    return Mono.error(new UnsupportedOperationException("requestResponse is not implemented"));
  }

  @Override
  public Flux<ServiceMessage> requestStream(ServiceMessage request) {
    return Flux.error(new UnsupportedOperationException("requestStream is not implemented"));
  }

  @Override
  public Mono<Void> fireAndForget(ServiceMessage request) {
    return Mono.error(new UnsupportedOperationException("fireAndForget is not implemented"));
  }

  @Override
  public Flux<ServiceMessage> requestChannel(Flux<ServiceMessage> request) {
    return Flux.error(new UnsupportedOperationException("requestChannel is not implemented"));
  }
}
