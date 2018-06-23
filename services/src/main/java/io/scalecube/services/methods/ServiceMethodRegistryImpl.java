package io.scalecube.services.methods;

import io.scalecube.services.Reflect;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ServiceMethodRegistryImpl implements ServiceMethodRegistry {

  private ConcurrentMap<String, ServiceMethodInvoker> methodInvokers = new ConcurrentHashMap<>();

  private ServiceMethodRegistryImpl() {
    // Do not instantiate
  }

  @Override
  public boolean containsInvoker(String qualifier) {
    return methodInvokers.containsKey(qualifier);
  }

  @Override
  public ServiceMethodInvoker getInvoker(String qualifier) {
    return methodInvokers.get(qualifier);
  }

  public static class Builder {
    private List<Object> services;

    private Builder() {}

    public Builder services(List<Object> services) {
      this.services = services;
      return this;
    }

    public ServiceMethodRegistryImpl build() {
      return new ServiceMethodRegistryImpl(this.services);
    }
  }

  public static Builder builder() {
    return new Builder();
  }

  private ServiceMethodRegistryImpl(List<Object> serviceObjects) {
    serviceObjects.forEach(service -> {
      Reflect.serviceInterfaces(service).forEach(serviceInterface -> {
        Reflect.serviceMethods(serviceInterface).forEach((key, method) -> {

          // validate method
          Reflect.validateMethodOrThrow(method);

          MethodInfo methodInfo = new MethodInfo(
              Reflect.serviceName(serviceInterface),
              Reflect.methodName(method),
              Reflect.parameterizedReturnType(method),
              Reflect.communicationMode(method),
              method.getParameterCount(),
              Reflect.requestType(method));

          // register new service method invoker
          methodInvokers.put(methodInfo.qualifier(),
              new ServiceMethodInvoker(method, service, methodInfo));
        });
      });
    });
  }
}
