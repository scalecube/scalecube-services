package io.scalecube.services;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;


public class ServiceDefinition {

  private final Class<?> serviceInterface;
  private final String serviceName;
 
  private final Map<String, Method> methods;

  /**
   * Constructor of service definition instance.
   * 
   * @param serviceInterface the class of the service interface.
   * @param serviceName - the qualifier of the service.
   * @param method - the method to invoke the service.
   */
  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Map<String, Method> methods) {
    this.serviceInterface = serviceInterface;
    this.serviceName = serviceName;
    this.methods = methods;
  }

  private Type extractReturnType(Type type) {
    if (type instanceof ParameterizedType) {
      return ((ParameterizedType) type).getActualTypeArguments()[0];
    } else {
      return Object.class;
    }
  }

  public Type returnType(String name) {
    return methods.get(name).getReturnType();
  }

  public Type parametrizedType(String name) {
    return extractReturnType(methods.get(name).getGenericReturnType());
  }

  public Class<?> serviceInterface() {
    return serviceInterface;
  }

  public String serviceName() {
    return serviceName;
  }

  
  public Method method(String name) {
    return  methods.get(name);
  }

  @Override
  public String toString() {
    return "ServiceDefinition [serviceInterface=" + serviceInterface
        + ", serviceName=" + serviceName
        + ", methods=" + methods
        + "]";
  }

  public Map<String, Method> methods() {
    return methods;
  }

}
