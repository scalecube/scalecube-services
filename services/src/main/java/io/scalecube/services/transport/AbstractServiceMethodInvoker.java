package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

import java.lang.reflect.Method;
import java.lang.reflect.Type;

public abstract class AbstractServiceMethodInvoker<REQ,RESP> implements ServiceMethodInvoker<REQ> {

  protected final Method method;

  protected final Object serviceObject;

  protected final Class requestType;

  protected final ServiceMessageCodec<?> payloadCodec;

  public AbstractServiceMethodInvoker(Object serviceObject,
      Method method,
      ServiceMessageCodec<?> payloadCodec) {

    this.serviceObject = serviceObject;
    this.method = method;
    this.requestType = Reflect.requestType(method);
    this.payloadCodec = payloadCodec;
  }

  protected ServiceMessage toMessage(Object obj) {
    return obj instanceof ServiceMessage ? (ServiceMessage) obj : ServiceMessage.builder().data(obj).build();
  }
}
