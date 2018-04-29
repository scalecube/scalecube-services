package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final ConcurrentMap<String, ServiceEndpoint> serviceEndpoints = new ConcurrentHashMap<>();

  @Override
  public List<ServiceEndpoint> listServiceEndpoints() {
    return new ArrayList<>(serviceEndpoints.values());
  }

  @Override
  public List<ServiceReference> listServiceReferences() {
    return serviceReferenceStream().collect(Collectors.toList());
  }

  @Override
  public List<ServiceReference> lookupService(String namespace) {
    return lookupService(r -> namespace.equalsIgnoreCase(r.namespace()));
  }

  @Override
  public List<ServiceReference> lookupService(Predicate<? super ServiceReference> filter) {
    // Convert to stream of service references
    Stream<ServiceReference> stream = serviceReferenceStream();

    // Filter by filter
    if (filter != null) {
      stream = stream.filter(filter);
    }

    // Collect results
    return stream.collect(Collectors.toList());
  }

  @Override
  public boolean registerService(ServiceEndpoint serviceEndpoint) {
    return serviceEndpoints.putIfAbsent(serviceEndpoint.id(), serviceEndpoint) == null;
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    return serviceEndpoints.remove(endpointId);
  }

  private Stream<ServiceReference> serviceReferenceStream() {
    return serviceEndpoints.values().stream().flatMap(
        se -> se.serviceRegistrations().stream().flatMap(
            sr -> sr.methods().stream().map(
                sm -> new ServiceReference(sm, sr, se))));
  }
}
