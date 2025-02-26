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
import java.lang.reflect.Method;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.function.Predicate;
import java.util.function.UnaryOperator;
import java.util.stream.Stream;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import reactor.core.scheduler.Scheduler;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryImpl.class);

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new ConcurrentHashMap<>();
  private final List<ServiceInfo> serviceInfos = new CopyOnWriteArrayList<>();

  // remote service references by static and dynamic qualifiers

  private final Map<String, List<ServiceReference>> serviceReferencesByQualifier =
      new ConcurrentHashMap<>();
  private final Map<DynamicQualifier, List<ServiceReference>> serviceReferencesByPattern =
      new ConcurrentHashMap<>();

  // local service method invokers by static and dynamic qualifiers

  private final Map<String, List<ServiceMethodInvoker>> methodInvokersByQualifier =
      new ConcurrentHashMap<>();
  private final Map<DynamicQualifier, List<ServiceMethodInvoker>> methodInvokersByPattern =
      new ConcurrentHashMap<>();

  public ServiceRegistryImpl() {}

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
    final var dataFormat = request.dataFormatOrDefault();
    final var qualifier = request.qualifier();
    final var requestMethod = request.requestMethod();

    // Match by exact-match

    final var list = serviceReferencesByQualifier.get(qualifier);
    if (list != null) {
      return list.stream()
          .filter(byDataFormat(dataFormat))
          .filter(byRequestMethod(requestMethod))
          .toList();
    }

    // Match by dynamic-qualifier

    for (var entry : serviceReferencesByPattern.entrySet()) {
      final var dynamicQualifier = entry.getKey();
      final var serviceReferences = entry.getValue();
      if (dynamicQualifier.matchQualifier(qualifier) != null) {
        return serviceReferences.stream()
            .filter(byDataFormat(dataFormat))
            .filter(byRequestMethod(requestMethod))
            .toList();
      }
    }

    return Collections.emptyList();
  }

  @Override
  public void registerService(ServiceEndpoint serviceEndpoint) {
    boolean putIfAbsent =
        serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (putIfAbsent) {
      serviceEndpoint.serviceReferences().forEach(this::addServiceReference);
      LOGGER.debug("ServiceEndpoint registered: {}", serviceEndpoint);
    }
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

      LOGGER.debug("ServiceEndpoint unregistered: {}", serviceEndpoint);
    }
    return serviceEndpoint;
  }

  @Override
  public void registerService(ServiceInfo serviceInfo) {
    registerService(serviceInfo, null, null);
  }

  @Override
  public void registerService(
      ServiceInfo serviceInfo,
      Map<String, Scheduler> schedulers,
      UnaryOperator<String> qualifierOperator) {
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

                          MethodInfo methodInfo =
                              new MethodInfo(
                                  Reflect.serviceName(serviceInterface),
                                  qualifierOperator != null
                                      ? qualifierOperator.apply(Reflect.methodName(method))
                                      : Reflect.methodName(method),
                                  Reflect.parameterizedReturnType(method),
                                  Reflect.isReturnTypeServiceMessage(method),
                                  Reflect.communicationMode(method),
                                  method.getParameterCount(),
                                  Reflect.requestType(method),
                                  Reflect.isRequestTypeServiceMessage(method),
                                  Reflect.isSecured(method),
                                  Reflect.executeOnScheduler(serviceMethod, schedulers),
                                  Reflect.restMethod(method),
                                  Reflect.allowedRoles(method),
                                  Reflect.allowedPermissions(method));

                          checkMethodInfo(methodInfo);

                          final var methodInvoker =
                              new ServiceMethodInvoker(
                                  method,
                                  serviceInstance,
                                  methodInfo,
                                  serviceInfo.errorMapper(),
                                  serviceInfo.dataDecoder(),
                                  serviceInfo.authenticator(),
                                  serviceInfo.logger());

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
  public ServiceMethodInvoker lookupInvoker(ServiceMessage request) {
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

  private static Predicate<ServiceReference> byDataFormat(String dataFormat) {
    return serviceReference -> serviceReference.contentTypes().contains(dataFormat);
  }

  private static Predicate<ServiceReference> byRequestMethod(String requestMethod) {
    return serviceReference -> {
      final var restMethod = serviceReference.restMethod();
      return restMethod == null || restMethod.equals(requestMethod);
    };
  }
}
