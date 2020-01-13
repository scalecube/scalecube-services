package io.scalecube.services.methods;

import io.scalecube.services.Reflect;
import io.scalecube.services.auth.Authenticator;
import io.scalecube.services.exceptions.ServiceProviderErrorMapper;
import io.scalecube.services.transport.api.ServiceMessageDataDecoder;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;

public final class ServiceMethodRegistryImpl implements ServiceMethodRegistry {

  private final ConcurrentMap<String, ServiceMethodInvoker> methodInvokers =
      new ConcurrentHashMap<>();

  @SuppressWarnings("rawtypes")
  @Override
  public void registerService(
      Object serviceInstance,
      ServiceProviderErrorMapper errorMapper,
      ServiceMessageDataDecoder dataDecoder,
      Authenticator authenticator) {
    Reflect.serviceInterfaces(serviceInstance)
        .forEach(
            serviceInterface -> {
              Map<String, Method> serviceMethods = Reflect.serviceMethods(serviceInterface);
              serviceMethods.forEach(
                  (key, method) -> {

                    // validate method
                    Reflect.validateMethodOrThrow(method);

                    MethodInfo methodInfo =
                        new MethodInfo(
                            Reflect.serviceName(serviceInterface),
                            Reflect.methodName(method),
                            Reflect.parameterizedReturnType(method),
                            Reflect.isReturnTypeServiceMessage(method),
                            Reflect.communicationMode(method),
                            method.getParameterCount(),
                            Reflect.requestType(method),
                            Reflect.isRequestTypeServiceMessage(method),
                            Reflect.isAuth(method));

                    // register new service method invoker
                    String qualifier = methodInfo.qualifier();
                    if (methodInvokers.containsKey(qualifier)) {
                      throw new IllegalStateException(
                          String.format("MethodInvoker for api '%s' already exists", qualifier));
                    }
                    ServiceMethodInvoker invoker =
                        new ServiceMethodInvoker(
                            method,
                            serviceInstance,
                            methodInfo,
                            errorMapper,
                            dataDecoder,
                            authenticator);
                    methodInvokers.put(methodInfo.qualifier(), invoker);
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

  @Override
  public List<ServiceMethodInvoker> listInvokers() {
    return new ArrayList<>(methodInvokers.values());
  }
}
