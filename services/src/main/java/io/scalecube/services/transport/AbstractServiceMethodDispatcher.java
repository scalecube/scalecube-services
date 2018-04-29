package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;

import java.lang.reflect.Method;

public abstract class AbstractServiceMethodDispatcher<REQ, RESP> implements ServiceMethodDispatcher<REQ> {

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
}
