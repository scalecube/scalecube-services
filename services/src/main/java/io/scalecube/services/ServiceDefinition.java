package io.scalecube.services;

import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Collections;
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
   * @param methods - the methods to invoke the service.
   */
  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Map<String, Method> methods) {
    this.serviceInterface = serviceInterface;
    this.serviceName = serviceName;
    this.methods = Collections.unmodifiableMap(methods);
  }

  public Class<?> serviceInterface() {
    return serviceInterface;
  }

  public String serviceName() {
    return serviceName;
  }

  public Method method(String name) {
    return methods.get(name);
  }

  public Map<String, Method> methods() {
    return methods;
  }

  @Override
  public String toString() {
    return "ServiceDefinition [serviceInterface=" + serviceInterface
        + ", serviceName=" + serviceName
        + ", methods=" + methods
        + "]";
  }

}
