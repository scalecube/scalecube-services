package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.lang.reflect.Type;
import java.util.Optional;

public class LocalServiceInstance implements ServiceInstance {

  private final Object serviceObject;
  private final Method method;
  private final String serviceName;
  private String[] tags;
  private final String memberId;

  /**
   * LocalServiceInstance instance constructor. 
   * @param serviceObject the instance of the service object. 
   * @param memberId the Cluster memberId of this instance. 
   * @param serviceInterface the service interface class of the service.
   * @param serviceName the qulifier name of the service.
   * @param method the java method of the service.
   * @param tags optional tags of the service.
   * @param returnType the return type class of the service method.
   */
  public LocalServiceInstance(
      Object serviceObject,
      String memberId,
      Class<?> serviceInterface,
      String serviceName,
      Method method,
      String[] tags,
      Type returnType) {

    checkArgument(serviceObject != null);
    checkArgument(memberId != null);
    checkArgument(serviceName != null);
    checkArgument(method != null);

    this.serviceObject = serviceObject;
    this.serviceName = serviceName;
    this.method = method;
    this.tags = tags;
    this.memberId = memberId;
  }


  @Override
  public Object invoke(Message message, ServiceDefinition definition)
      throws InvocationTargetException, IllegalAccessException {
    checkArgument(message != null);

    Method method = this.method;
    Object result;

    if (method.getParameters().length == 0) {
      result = method.invoke(serviceObject);
    } else if (method.getParameters()[0].getType().isAssignableFrom(Message.class)) {
      if (message.data().getClass().isAssignableFrom(Message.class)) {
        result = method.invoke(serviceObject, (Message) message.data());
      } else {
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

  public String serviceName() {
    return serviceName;
  }

  @Override
  public String memberId() {
    return this.memberId;
  }

  @Override
  public Boolean isLocal() {
    return true;
  }

  @Override
  public boolean isReachable() {
    return true;
  }
  
  @Override
  public String toString() {
    return "LocalServiceInstance [serviceObject=" + serviceObject + ", memberId=" + memberId + "]";
  }
}
