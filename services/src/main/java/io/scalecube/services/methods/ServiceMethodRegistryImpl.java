package io.scalecube.services.methods;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceInfo;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public final class ServiceMethodRegistryImpl implements ServiceMethodRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceMethodRegistry.class);

  private final List<ServiceInfo> serviceInfos = new CopyOnWriteArrayList<>();

  private final ConcurrentMap<String, ServiceMethodInvoker> methodInvokers =
      new ConcurrentHashMap<>();

  @Override
  public void registerService(ServiceInfo serviceInfo) {
    serviceInfos.add(serviceInfo);

    Reflect.serviceInterfaces(serviceInfo.serviceInstance())
        .forEach(
            serviceInterface ->
                Reflect.serviceMethods(serviceInterface)
                    .forEach(
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
                                  Reflect.isSecured(method));

                          checkMethodInvokerDoesntExist(methodInfo);

                          ServiceMethodInvoker methodInvoker =
                              new ServiceMethodInvoker(
                                  method,
                                  serviceInfo.serviceInstance(),
                                  methodInfo,
                                  serviceInfo.errorMapper(),
                                  serviceInfo.dataDecoder(),
                                  serviceInfo.authenticator(),
                                  serviceInfo.principalMapper());

                          methodInvokers.put(methodInfo.qualifier(), methodInvoker);
                          methodInvokers.put(methodInfo.oldQualifier(), methodInvoker);
                        }));
  }

  private void checkMethodInvokerDoesntExist(MethodInfo methodInfo) {
    if (methodInvokers.containsKey(methodInfo.qualifier())
        || methodInvokers.containsKey(methodInfo.oldQualifier())) {
      LOGGER.error("MethodInvoker already exists, methodInfo: {}", methodInfo);
      throw new IllegalStateException("MethodInvoker already exists");
    }
  }

  @Override
  public ServiceMethodInvoker getInvoker(String qualifier) {
    return methodInvokers.get(Objects.requireNonNull(qualifier, "[getInvoker] qualifier"));
  }

  @Override
  public List<ServiceMethodInvoker> listInvokers() {
    return new ArrayList<>(methodInvokers.values());
  }

  @Override
  public List<ServiceInfo> listServices() {
    return new ArrayList<>(serviceInfos);
  }
}
