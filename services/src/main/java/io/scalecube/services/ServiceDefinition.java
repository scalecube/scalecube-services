package io.scalecube.services;

import java.lang.reflect.Method;


public class ServiceDefinition {

  private final Class<?> serviceInterface;
  private final String qualifier;
  private final Method method;

  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Method method) {
    this.serviceInterface = serviceInterface;
    this.qualifier = serviceName;
    this.method = method;
  }

  public Class<?> serviceInterface() {
    return serviceInterface;
  }

  public String qualifier() {
    return qualifier;
  }

  public Method method() {
    return this.method;
  }

  @Override
  public String toString() {
    return "ServiceDefinition [serviceInterface=" + serviceInterface +
        ", qualifier=" + qualifier +
        ", method=" + method
        + "]";
  }

  public static ServiceInstance toLocalServiceInstance(ServiceDefinition def, Object serviceObject, String memberId,
      String[] tags) {
    return new LocalServiceInstance(serviceObject,
        memberId,
        def.serviceInterface(),
        def.qualifier(),
        def.method(),
        tags);
  }
}
