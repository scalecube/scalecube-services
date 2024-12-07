package io.scalecube.services.registry;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.lang.System.Logger;
import java.lang.System.Logger.Level;
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.regex.Pattern;
import java.util.stream.Collectors;
import org.jctools.maps.NonBlockingHashMap;
import reactor.core.scheduler.Scheduler;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = System.getLogger(ServiceRegistryImpl.class.getName());

  private final Map<String, Scheduler> schedulers;

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> serviceReferencesByQualifier =
      new NonBlockingHashMap<>();
  private final Map<String, ServiceMethodInvoker> methodInvokerByQualifier =
      new NonBlockingHashMap<>();
  private final Map<Pattern, List<ServiceReference>> serviceReferencesByPattern =
      new NonBlockingHashMap<>();
  private final Map<Pattern, ServiceMethodInvoker> methodInvokerByPattern =
      new NonBlockingHashMap<>();
  private final List<ServiceInfo> serviceInfos = new CopyOnWriteArrayList<>();

  public ServiceRegistryImpl(Map<String, Scheduler> schedulers) {
    this.schedulers = schedulers;
  }

  @Override
  public List<ServiceEndpoint> listServiceEndpoints() {
    // todo how to collect tags correctly?
    return new ArrayList<>(serviceEndpoints.values());
  }

  @Override
  public List<ServiceReference> listServiceReferences() {
    return serviceReferencesByQualifier.values().stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  public List<ServiceReference> lookupService(ServiceMessage request) {
    List<ServiceReference> list = serviceReferencesByQualifier.get(request.qualifier());
    if (list == null || list.isEmpty()) {
      return Collections.emptyList();
    }
    String contentType = request.dataFormatOrDefault();
    return list.stream()
        .filter(sr -> sr.contentTypes().contains(contentType))
        .collect(Collectors.toList());
  }

  @Override
  public boolean registerService(ServiceEndpoint serviceEndpoint) {
    boolean putIfAbsent =
        serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (putIfAbsent) {
      LOGGER.log(Level.DEBUG, "ServiceEndpoint registered: {0}", serviceEndpoint);
      serviceEndpoint.serviceReferences().forEach(this::addServiceReference);
    }
    return putIfAbsent;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      LOGGER.log(Level.DEBUG, "ServiceEndpoint unregistered: {0}", serviceEndpoint);

      serviceReferencesByQualifier.values().stream()
          .flatMap(Collection::stream)
          .filter(sr -> sr.endpointId().equals(endpointId))
          .forEach(this::removeServiceReference);
    }
    return serviceEndpoint;
  }

  @Override
  public void registerService(ServiceInfo serviceInfo) {
    serviceInfos.add(serviceInfo);

    final var serviceInstance = serviceInfo.serviceInstance();
    Reflect.serviceInterfaces(serviceInstance)
        .forEach(
            serviceInterface ->
                Reflect.serviceMethods(serviceInterface)
                    .forEach(
                        (key, method) -> {

                          // validate method
                          Reflect.validateMethodOrThrow(method);

                          // get service instance method
                          Method serviceMethod;
                          try {
                            serviceMethod =
                                serviceInstance
                                    .getClass()
                                    .getMethod(method.getName(), method.getParameterTypes());
                          } catch (NoSuchMethodException e) {
                            throw new RuntimeException(e);
                          }

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
                                  Reflect.isSecured(method),
                                  Reflect.executeOnScheduler(serviceMethod, schedulers));

                          checkMethodInvokerIsNotPresent(methodInfo);

                          ServiceMethodInvoker methodInvoker =
                              new ServiceMethodInvoker(
                                  method,
                                  serviceInstance,
                                  methodInfo,
                                  serviceInfo.errorMapper(),
                                  serviceInfo.dataDecoder(),
                                  serviceInfo.authenticator(),
                                  serviceInfo.principalMapper(),
                                  serviceInfo.logger(),
                                  serviceInfo.level());

                          methodInvokerByQualifier.put(methodInfo.qualifier(), methodInvoker);
                        }));
  }

  private void checkMethodInvokerIsNotPresent(MethodInfo methodInfo) {
    if (methodInvokerByQualifier.containsKey(methodInfo.qualifier())) {
      LOGGER.log(Level.ERROR, "MethodInvoker already exists, methodInfo: {0}", methodInfo);
      throw new IllegalStateException("MethodInvoker already exists");
    }
  }

  @Override
  public ServiceMethodInvoker getInvoker(String qualifier) {
    return methodInvokerByQualifier.get(qualifier);
  }

  @Override
  public List<ServiceInfo> listServices() {
    return serviceInfos;
  }

  private void addServiceReference(ServiceReference sr) {
    serviceReferencesByQualifier
        .computeIfAbsent(sr.qualifier(), key -> new CopyOnWriteArrayList<>())
        .add(sr);
  }

  private void removeServiceReference(ServiceReference sr) {
    serviceReferencesByQualifier.compute(
        sr.qualifier(),
        (key, list) -> {
          if (list == null || list.isEmpty()) {
            return null;
          }
          list.remove(sr);
          return list.isEmpty() ? null : list;
        });
  }
}
