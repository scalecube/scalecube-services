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
import java.util.Objects;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Stream;
import org.jctools.maps.NonBlockingHashMap;
import reactor.core.scheduler.Scheduler;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = System.getLogger(ServiceRegistryImpl.class.getName());

  private final Map<String, Scheduler> schedulers;

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final List<ServiceInfo> serviceInfos = new CopyOnWriteArrayList<>();

  // remote service references by static and dynamic qualifiers

  private final Map<String, List<ServiceReference>> serviceReferencesByQualifier =
      new NonBlockingHashMap<>();
  private final Map<DynamicQualifier, List<ServiceReference>> serviceReferencesByPattern =
      new NonBlockingHashMap<>();

  // local service method invokers by static and dynamic qualifiers

  private final Map<String, List<ServiceMethodInvoker>> methodInvokersByQualifier =
      new NonBlockingHashMap<>();
  private final Map<DynamicQualifier, List<ServiceMethodInvoker>> methodInvokersByPattern =
      new NonBlockingHashMap<>();

  /**
   * Constructor
   *
   * @param schedulers schedulers (optiona)
   */
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
    return Stream.concat(
            serviceReferencesByQualifier.values().stream().flatMap(Collection::stream),
            serviceReferencesByPattern.values().stream().flatMap(Collection::stream))
        .toList();
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

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      // Clean exact-match service references

      serviceReferencesByQualifier.values().stream()
          .flatMap(Collection::stream)
          .filter(sr -> sr.endpointId().equals(endpointId))
          .forEach(
              value ->
                  serviceReferencesByQualifier.compute(
                      value.qualifier(), (key, list) -> removeServiceReference(value, list)));

      // Clean dynamic-qualifier service references

      serviceReferencesByPattern.values().stream()
          .flatMap(Collection::stream)
          .filter(sr -> sr.endpointId().equals(endpointId))
          .forEach(
              value ->
                  serviceReferencesByPattern.compute(
                      value.dynamicQualifier(),
                      (key, list) -> removeServiceReference(value, list)));

      LOGGER.log(Level.DEBUG, "ServiceEndpoint unregistered: {0}", serviceEndpoint);
    }
    return serviceEndpoint;
  }

  @Override
  public void registerService(ServiceInfo serviceInfo) {
    final var serviceInstance = serviceInfo.serviceInstance();
    final var serviceInstanceClass = serviceInstance.getClass();

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
                                serviceInstanceClass.getMethod(
                                    method.getName(), method.getParameterTypes());
                          } catch (NoSuchMethodException e) {
                            throw new RuntimeException(e);
                          }

                          final var methodInfo =
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
                                  Reflect.executeOnScheduler(serviceMethod, schedulers),
                                  Reflect.restMethod(method));

                          checkMethodInfo(methodInfo);

                          final var methodInvoker =
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

                          final List<ServiceMethodInvoker> methodInvokers;
                          if (methodInfo.dynamicQualifier() == null) {
                            methodInvokers =
                                methodInvokersByQualifier.computeIfAbsent(
                                    methodInfo.qualifier(), s -> new ArrayList<>());
                          } else {
                            methodInvokers =
                                methodInvokersByPattern.computeIfAbsent(
                                    methodInfo.dynamicQualifier(), s -> new ArrayList<>());
                          }
                          methodInvokers.add(methodInvoker);
                        }));

    serviceInfos.add(serviceInfo);
  }

  private void checkMethodInfo(MethodInfo methodInfo) {
    final var restMethod = methodInfo.restMethod(); // nullable

    if (methodInfo.qualifier() != null) {
      final var hasMethodInvokerByQualifier =
          methodInvokersByQualifier.getOrDefault(methodInfo.qualifier(), List.of()).stream()
              .filter(smi -> Objects.equals(smi.methodInfo().restMethod(), restMethod))
              .findAny();
      if (hasMethodInvokerByQualifier.isPresent()) {
        throw new IllegalStateException("MethodInvoker already exists, methodInfo: " + methodInfo);
      }
    }

    if (methodInfo.dynamicQualifier() != null) {
      final var hasMethodInvokerByDynamicQualifier =
          methodInvokersByPattern.getOrDefault(methodInfo.dynamicQualifier(), List.of()).stream()
              .filter(smi -> Objects.equals(smi.methodInfo().restMethod(), restMethod))
              .findAny();
      if (hasMethodInvokerByDynamicQualifier.isPresent()) {
        throw new IllegalStateException("MethodInvoker already exists, methodInfo: " + methodInfo);
      }
    }
  }

  @Override
  public ServiceMethodInvoker getInvoker(ServiceMessage request) {
    final var qualifier = request.qualifier();
    final var requestMethod = request.requestMethod();

    // Match by exact-match

    final var methodInvokers = methodInvokersByQualifier.get(qualifier);
    if (methodInvokers != null) {
      for (var methodInvoker : methodInvokers) {
        final var restMethod = methodInvoker.methodInfo().restMethod();
        if (restMethod == null || restMethod.equals(requestMethod)) {
          return methodInvoker;
        }
      }
    }

    // Match by dynamic-qualifier

    for (var entry : methodInvokersByPattern.entrySet()) {
      for (var methodInvoker : entry.getValue()) {
        final var methodInfo = methodInvoker.methodInfo();
        final var restMethod = methodInfo.restMethod();
        if (restMethod == null || restMethod.equals(requestMethod)) {
          if (methodInfo.dynamicQualifier().matchQualifier(qualifier) != null) {
            return methodInvoker;
          }
        }
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

  private static List<ServiceReference> removeServiceReference(
      ServiceReference value, List<ServiceReference> list) {
    if (list == null || list.isEmpty()) {
      return null;
    }
    list.remove(value);
    return list.isEmpty() ? null : list;
  }
}
