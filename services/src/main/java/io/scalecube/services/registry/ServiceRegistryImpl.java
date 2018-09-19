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
import java.util.function.Function;
import java.util.stream.Collectors;
import org.jctools.maps.NonBlockingHashMap;

public class ServiceRegistryImpl implements ServiceRegistry {

  // todo how to remove it (tags problem)?
  private final Map<String, ServiceEndpoint> serviceEndpoints = new NonBlockingHashMap<>();
  private final Map<String, List<ServiceReference>> referencesByQualifier =
      new NonBlockingHashMap<>();

  @Override
  public List<ServiceEndpoint> listServiceEndpoints() {
    // todo how to collect tags correctly?
    return new ArrayList<>(serviceEndpoints.values());
  }

  @Override
  public List<ServiceReference> listServiceReferences() {
    return referencesByQualifier
        .values()
        .stream()
        .flatMap(Collection::stream)
        .collect(Collectors.toList());
  }

  @Override
  public List<ServiceReference> lookupService(ServiceMessage request) {
    List<ServiceReference> result = referencesByQualifier.get(request.qualifier());
    if (result == null || result.isEmpty()) {
      return Collections.emptyList();
    }
    String contentType = request.dataFormatOrDefault();
    return result
        .stream()
        .filter(ref -> ref.contentTypes().contains(contentType))
        .collect(Collectors.toList());
  }

  @Override
  public boolean registerService(ServiceEndpoint serviceEndpoint) {
    boolean success = serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
    if (success) {
      serviceEndpoint
          .serviceReferences()
          .forEach(
              sr ->
                  referencesByQualifier
                      .computeIfAbsent(sr.qualifier(), key -> new CopyOnWriteArrayList<>())
                      .add(sr));
    }
    return success;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    ServiceEndpoint serviceEndpoint = serviceEndpoints.remove(endpointId);
    if (serviceEndpoint != null) {

      Map<String, ServiceReference> serviceReferencesOfEndpoint =
          referencesByQualifier
              .values()
              .stream()
              .flatMap(Collection::stream)
              .filter(sr -> sr.endpointId().equals(endpointId))
              .collect(Collectors.toMap(ServiceReference::qualifier, Function.identity()));

      serviceReferencesOfEndpoint.forEach(
          (qualifier, sr) -> {
            // do remapping
            referencesByQualifier.compute(
                qualifier,
                (qualifier1, list) -> {
                  list.remove(sr);
                  return !list.isEmpty() ? list : null;
                });
          });
    }
    return serviceEndpoint;
  }
}
