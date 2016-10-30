package io.scalecube.services;

import java.lang.reflect.Method;
import java.lang.reflect.Type;


public class ServiceDefinition {


  private final Class<?> serviceInterface;
  private final String qualifier;
  private final Method method;

  private final Type returnType;
  private final Type parameterizedType;

  /**
   * Constructor of service definition instance.
   * @param serviceInterface the class of the service interface.
   * @param serviceName - the qualifier of the service.
   * @param method - the method to invoke the service.
   * @param returnType the type of the expected result.
   * @param parameterizedType the type of the generic class of the result (if any).
   */
  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Method method, Type returnType,
      Type parameterizedType) {
    this.serviceInterface = serviceInterface;
    this.qualifier = serviceName;
    this.method = method;
    this.returnType = returnType;
    this.parameterizedType = parameterizedType;
  }

  public Type returnType() {
    return returnType;
  }

  public Type parameterizedType() {
    return parameterizedType;
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
    return "ServiceDefinition [serviceInterface=" + serviceInterface + ", qualifier=" + qualifier + ", method=" + method
        + "]";
  }

  public static ServiceInstance toLocalServiceInstance(ServiceDefinition def, Object serviceObject, String memberId,
      String[] tags, Type returnType) {
    return new LocalServiceInstance(serviceObject,
        memberId,
        def.serviceInterface(),
        def.qualifier(),
        def.method(),
        tags,
        returnType);
  }



}
