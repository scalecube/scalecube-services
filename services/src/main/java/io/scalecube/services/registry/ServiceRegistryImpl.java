package io.scalecube.services.registry;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceInfo;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.methods.MethodInfo;
import io.scalecube.services.methods.ServiceMethodInvoker;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.stream.Collectors;
import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ServiceRegistryImpl implements ServiceRegistry {

  private static final Logger LOGGER = LoggerFactory.getLogger(ServiceRegistryImpl.class);

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> serviceReferencesByQualifier =
      new NonBlockingHashMap<>();
  private final List<ServiceInfo> serviceInfos = new CopyOnWriteArrayList<>();
  private final ConcurrentMap<String, ServiceMethodInvoker> methodInvokers =
      new ConcurrentHashMap<>();

  public ServiceRegistryImpl() {}

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
    boolean success = serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (success) {
      LOGGER.debug("ServiceEndpoint registered: {}", serviceEndpoint);
      serviceEndpoint
          .serviceReferences()
          .forEach(
              sr -> {
                populateServiceReferences(sr.qualifier(), sr);
                populateServiceReferences(sr.oldQualifier(), sr);
              });
    }
    return success;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {
      LOGGER.debug("ServiceEndpoint unregistered: {}", serviceEndpoint);

      List<ServiceReference> serviceReferencesOfEndpoint =
          serviceReferencesByQualifier.values().stream()
              .flatMap(Collection::stream)
              .filter(sr -> sr.endpointId().equals(endpointId))
              .collect(Collectors.toList());

      serviceReferencesOfEndpoint.forEach(
          sr -> {
            computeServiceReferences(sr.qualifier(), sr);
            computeServiceReferences(sr.oldQualifier(), sr);
          });
    }
    return serviceEndpoint;
  }

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
  public List<ServiceInfo> listServices() {
    return serviceInfos;
  }

  private void populateServiceReferences(String qualifier, ServiceReference serviceReference) {
    serviceReferencesByQualifier
        .computeIfAbsent(qualifier, key -> new CopyOnWriteArrayList<>())
        .add(serviceReference);
  }

  private void computeServiceReferences(String qualifier, ServiceReference serviceReference) {
    serviceReferencesByQualifier.compute(
        qualifier,
        (key, list) -> {
          if (list == null || list.isEmpty()) {
            return null;
          }
          list.remove(serviceReference);
          return !list.isEmpty() ? list : null;
        });
  }
}
