package io.scalecube.services.registry;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.DynamicQualifier;
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
  private final Map<DynamicQualifier, List<ServiceReference>> serviceReferencesByPattern =
      new NonBlockingHashMap<>();
  private final Map<DynamicQualifier, ServiceMethodInvoker> methodInvokerByPattern =
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
    return serviceReferencesByQualifier.values().stream().flatMap(Collection::stream).toList();
  }

  @Override
  public List<ServiceReference> lookupService(ServiceMessage request) {
    final var contentType = request.dataFormatOrDefault();
    final var qualifier = request.qualifier();

    // Match by exact-match

    final var list = serviceReferencesByQualifier.get(qualifier);
    if (list != null) {
      return list.stream().filter(sr -> sr.contentTypes().contains(contentType)).toList();
    }

    // Match by dynamic-qualifier

    for (var entry : serviceReferencesByPattern.entrySet()) {
      final var dynamicQualifier = entry.getKey();
      if (dynamicQualifier.matchQualifier(qualifier) != null) {
        return entry.getValue();
      }
    }

    return Collections.emptyList();
  }

  @Override
  public boolean registerService(ServiceEndpoint serviceEndpoint) {
    boolean putIfAbsent =
        serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (putIfAbsent) {
      serviceEndpoint.serviceReferences().forEach(this::addServiceReference);
      LOGGER.log(Level.DEBUG, "ServiceEndpoint registered: {0}", serviceEndpoint);
    }
    return putIfAbsent;
  }

  // TODO: refactor, clean also serviceReferencesByQualifier
  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      serviceReferencesByQualifier.values().stream()
          .flatMap(Collection::stream)
          .filter(sr -> sr.endpointId().equals(endpointId))
          .forEach(this::removeServiceReference);
      LOGGER.log(Level.DEBUG, "ServiceEndpoint unregistered: {0}", serviceEndpoint);
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

                          if (methodInfo.dynamicQualifier() == null) {
                            methodInvokerByQualifier.put(methodInfo.qualifier(), methodInvoker);
                          } else {
                            methodInvokerByPattern.put(
                                methodInfo.dynamicQualifier(), methodInvoker);
                          }
                        }));
  }

  private void checkMethodInvokerIsNotPresent(MethodInfo methodInfo) {
    if (methodInvokerByQualifier.containsKey(methodInfo.qualifier())) {
      LOGGER.log(Level.ERROR, "MethodInvoker already exists, methodInfo: {0}", methodInfo);
      throw new IllegalStateException("MethodInvoker already exists");
    }
    if (methodInfo.dynamicQualifier() != null) {
      if (methodInvokerByPattern.containsKey(methodInfo.dynamicQualifier())) {
        LOGGER.log(Level.ERROR, "MethodInvoker already exists, methodInfo: {0}", methodInfo);
        throw new IllegalStateException("MethodInvoker already exists");
      }
    }
  }

  @Override
  public ServiceMethodInvoker getInvoker(String qualifier) {
    // Match by exact-match

    final var methodInvoker = methodInvokerByQualifier.get(qualifier);
    if (methodInvoker != null) {
      return methodInvoker;
    }

    // Match by dynamic-qualifier

    for (var entry : methodInvokerByPattern.entrySet()) {
      final var invoker = entry.getValue();
      final var dynamicQualifier = invoker.methodInfo().dynamicQualifier();
      if (dynamicQualifier.matchQualifier(qualifier) != null) {
        return invoker;
      }
    }

    return null;
  }

  @Override
  public List<ServiceInfo> listServices() {
    return new ArrayList<>(serviceInfos);
  }

  private void addServiceReference(ServiceReference sr) {
    if (sr.dynamicQualifier() == null) {
      serviceReferencesByQualifier
          .computeIfAbsent(sr.qualifier(), key -> new CopyOnWriteArrayList<>())
          .add(sr);
    } else {
      serviceReferencesByPattern
          .computeIfAbsent(sr.dynamicQualifier(), key -> new CopyOnWriteArrayList<>())
          .add(sr);
    }
  }

  // TODO: refactor, clean also serviceReferencesByQualifier
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
