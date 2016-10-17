package io.scalecube.services;

import java.lang.reflect.Method;


public class ServiceDefinition {

  private final Class<?> serviceInterface;
  private final String qualifier;
  private final Method method;
  private final Class<?> routing;
  
  public Class<?> routing() {
    return routing;
  }

  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Method method, Class<?> routing) {
    this.serviceInterface = serviceInterface;
    this.qualifier = serviceName;
    this.method = method;
    this.routing = routing;
  }

  public Class<?> serviceInterface() {
    return serviceInterface;
  }

  public String qualifier() {
    return qualifier;
  }
  
  public Method method(){
    return this.method;
  }

  @Override
  public String toString() {
    return "{service='" + qualifier + "\'"  + '}';
  }
}
