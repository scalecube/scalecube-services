package io.scalecube.services;

import com.google.common.collect.BiMap;
import com.google.common.collect.ImmutableBiMap;

import java.lang.reflect.Method;
import java.util.Map;
import java.util.Set;

/**
 * @author Anton Kharenko
 */
public class ServiceDefinition {

  private final Class<?> serviceInterface;
  private final String serviceName;
  private final BiMap<String, Method> methods;

  public ServiceDefinition(Class<?> serviceInterface, String serviceName, Map<String, Method> methods) {
    this.serviceInterface = serviceInterface;
    this.serviceName = serviceName;
    this.methods = ImmutableBiMap.copyOf(methods);
  }

  public Class<?> serviceInterface() {
    return serviceInterface;
  }

  public String serviceName() {
    return serviceName;
  }

  public Set<String> methodNames() {
    return methods.keySet();
  }

  public String methodName(Method method) {
    return methods.inverse().get(method);
  }

  public Method method(String methodName) {
    return methods.get(methodName);
  }

  @Override
  public String toString() {
    return "{service='" + serviceName + "\', methods=" + methodNames() + '}';
  }
}
