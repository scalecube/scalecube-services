package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
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
