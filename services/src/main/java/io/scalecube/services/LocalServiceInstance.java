package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Optional;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.transport.Message;

public class LocalServiceInstance implements ServiceInstance {
  private static final Logger LOGGER = LoggerFactory.getLogger(LocalServiceInstance.class);

  @Override
  public String toString() {
    return "LocalServiceInstance [serviceObject=" + serviceObject + ", memberId=" + memberId + ", isLocal=" + isLocal
        + "]";
  }

  private final Object serviceObject;
  private final Method method;

  private final String qualifier;
  private String[] tags;

  private final String memberId;
  private final Boolean isLocal;

  public LocalServiceInstance(Object serviceObject, String memberId, Class<?> serviceInterface,
      String qualifier,
      Method method,
      String[] tags,
      Type returnType) {

    checkArgument(serviceObject != null);
    checkArgument(memberId != null);
    checkArgument(qualifier != null);
    checkArgument(method != null);

    this.serviceObject = serviceObject;
    this.qualifier = qualifier;
    this.method = method;
    this.tags = tags;
    this.memberId = memberId;

    this.isLocal = true;
  }


  @Override
  public <T> Object invoke(Message message, Optional<ServiceDefinition> definition)
      throws InvocationTargetException, IllegalAccessException {
    checkArgument(message != null);

    Method method = this.method;

    Object result = null;

    if (method.getParameters().length == 0) {
      result = method.invoke(serviceObject);
    } else if (method.getParameters()[0].getType().isAssignableFrom(Message.class)) {
      if (message.data().getClass().isAssignableFrom(Message.class))
        result = method.invoke(serviceObject, (Message) message.data());
      else {
        result = method.invoke(serviceObject, message);
      }
    } else {
      result = method.invoke(serviceObject, new Object[] {message.data()});
    }
    return result;
  }

  public String[] tags() {
    return tags;
  }

  public String qualifier() {
    return qualifier;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  @Override
  public Boolean isLocal() {
    return isLocal;
  }

  @Override
  public boolean isReachable() {
    return true;
  }
}
