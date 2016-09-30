package io.scalecube.services;

import java.lang.reflect.Method;


public class ServiceDefinition {

  private final Class<?> serviceInterface;
  private final String serviceName;
  private final Method method;
  
  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Method method) {
    this.serviceInterface = serviceInterface;
    this.serviceName = serviceName;
    this.method = method;
  }

  public Class<?> serviceInterface() {
    return serviceInterface;
  }

  public String serviceName() {
    return serviceName;
  }
  
  public Method method(){
    return this.method;
  }

  @Override
  public String toString() {
    return "{service='" + serviceName + "\'"  + '}';
  }
}
