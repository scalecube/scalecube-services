package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.codecs.api.ServiceMessageDataCodec;
import io.scalecube.services.transport.api.ServiceMethodDispatcher;

import java.lang.reflect.Method;

public abstract class AbstractServiceMethodDispatcher<REQ, RESP> implements ServiceMethodDispatcher<REQ> {

  protected final Method method;
  protected final Object serviceObject;
  protected final Class requestType;
  protected final ServiceMessageDataCodec payloadCodec;
  private final String methodName;

  @Override
  public ServiceMessageDataCodec getCodec() {
    return this.payloadCodec;
  }

  public String methodName() {
    return this.methodName;
  }

  public AbstractServiceMethodDispatcher(Object serviceObject,
      Method method,
      ServiceMessageDataCodec payloadCodec) {

    this.serviceObject = serviceObject;
    this.method = method;
    this.methodName = Reflect.methodName(method);
    this.requestType = Reflect.requestType(method);
    this.payloadCodec = payloadCodec;
  }

  protected ServiceMessage toReturnMessage(Object obj) {
    return obj instanceof ServiceMessage ? (ServiceMessage) obj
        : ServiceMessage.builder().header("_type", Reflect.parameterizedReturnType(method).getName())
            .data(obj)
            .build();
  }
}
