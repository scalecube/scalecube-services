package io.scalecube.services.transport;

import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;

import java.lang.reflect.Method;

public abstract class AbstractServiceMethodInvoker<REQ,RESP> implements ServiceMethodInvoker<REQ> {

  protected final Method method;

  protected final Object serviceObject;

  protected final Class<?> requestType;

  protected final Class<?> respType;

  protected final ServiceMessageCodec<?> payloadCodec;

  public AbstractServiceMethodInvoker(Object serviceObject,
      Method method,
      Class<?> reqType,
      Class<?> respType,
      ServiceMessageCodec<?> payloadCodec) {

    this.serviceObject = serviceObject;
    this.method = method;
    this.requestType = reqType;
    this.respType = respType;
    this.payloadCodec = payloadCodec;
  }

  protected ServiceMessage toMessage(Object obj) {
    return obj instanceof ServiceMessage ? (ServiceMessage) obj : ServiceMessage.builder().data(obj).build();
  }
}
