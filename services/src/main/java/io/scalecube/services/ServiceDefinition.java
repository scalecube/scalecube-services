package io.scalecube.services;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.List;


public class ServiceDefinition {

  private final Class<?> serviceInterface;
  private final String qualifier;
  private final Method method;
  private final Type returnType;
  private final Type parametrizedType;

  /**
   * Constructor of service definition instance.
   * @param serviceInterface the class of the service interface.
   * @param qualifier - the qualifier of the service.
   * @param method - the method to invoke the service.
   */
  public ServiceDefinition(Class<?> serviceInterface, String qualifier, Method method) {
    this.serviceInterface = serviceInterface;
    this.qualifier = qualifier;
    this.method = method;
    this.returnType = method.getReturnType();
    this.parametrizedType = extractReturnType(method.getGenericReturnType());
  }

  private Type extractReturnType(Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getActualTypeArguments()[0];
    } else {
      return Object.class;
    }
  }

  public Type returnType() {
    return returnType;
  }

  public Type parametrizedType() {
    return parametrizedType;
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

  /**
   * helper method to create a local service instance. 
   * @param def ServiceDefinition of the requested instance.
   * @param serviceObject the service object instance of the service.
   * @param memberId the Cluster memberId of this instance.
   * @param tags optional tags of the service.
   * @param returnType the return type of the service instance.
   * @return newly created service instance.
   */
  public static ServiceInstance toLocalServiceInstance(
      ServiceDefinition def, Object serviceObject, String memberId, Type returnType) {
    return new LocalServiceInstance(serviceObject,
        memberId,
        def.serviceInterface(),
        def.qualifier(),
        def.method(),
        returnType);
  }

  @Override
  public String toString() {
    return "ServiceDefinition [serviceInterface=" + serviceInterface
        + ", serviceName=" + qualifier
        + ", method=" + method
        + "]";
  }

}
