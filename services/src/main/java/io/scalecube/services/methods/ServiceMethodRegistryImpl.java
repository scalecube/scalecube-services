package io.scalecube.services.methods;

import io.scalecube.services.Reflect;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ServiceMethodRegistryImpl implements ServiceMethodRegistry {

  private final ConcurrentMap<String, ServiceMethodInvoker> methodInvokers = new ConcurrentHashMap<>();

  @Override
  public void registerService(Object serviceInstance) {
    Reflect.serviceInterfaces(serviceInstance).forEach(serviceInterface -> {
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
            new ServiceMethodInvoker(method, serviceInstance, methodInfo));
      });
    });
  }

  @Override
  public boolean containsInvoker(String qualifier) {
    return methodInvokers.containsKey(qualifier);
  }

  @Override
  public ServiceMethodInvoker getInvoker(String qualifier) {
    return methodInvokers.get(qualifier);
  }
}
