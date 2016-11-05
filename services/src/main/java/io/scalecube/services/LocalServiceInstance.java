package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Message;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;

/**
 * Local service instance invokes the service instance hosted on this local process.
 * 
 *
 */
public class LocalServiceInstance implements ServiceInstance {

  private final Object serviceObject;
  private final Method method;
  private final String serviceName;
  private final String memberId;

  /**
   * LocalServiceInstance instance constructor.
   * 
   * @param serviceObject the instance of the service object.
   * @param memberId the Cluster memberId of this instance.
   * @param serviceName the qualifier name of the service.
   * @param method the java method of the service.
   */
  public LocalServiceInstance(Object serviceObject, String memberId, String serviceName, Method method) {
    checkArgument(serviceObject != null);
    checkArgument(memberId != null);
    checkArgument(serviceName != null);
    checkArgument(method != null);
    this.serviceObject = serviceObject;
    this.serviceName = serviceName;
    this.method = method;
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
  public String toString() {
    return "LocalServiceInstance [serviceObject=" + serviceObject + ", memberId=" + memberId + "]";
  }
}
