package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.Collection;
import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;
import java.util.stream.Stream;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final ConcurrentMap<String, ServiceEndpoint> serviceEndpoints = new ConcurrentHashMap<>();

  @Override
  public ServiceEndpoint registerService(ServiceEndpoint serviceEndpoint) {
    String endpointId = serviceEndpoint.endpointId();
    return serviceEndpoints.compute(endpointId, (k, oldServiceEndpoint) -> serviceEndpoint);
  }

  @Override
  public ServiceEndpoint unregisterService(String endpointId) {
    return serviceEndpoints.remove(endpointId);
  }

  @Override
  public Collection<ServiceEndpoint> listServices() {
    return serviceEndpoints.values();
  }

  @Override
  public List<ServiceReference> serviceLookup(String serviceName) {
    return serviceLookup(r -> serviceName.equalsIgnoreCase(r.serviceName()));
  }

  @Override
  public List<ServiceReference> serviceLookup(Predicate<? super ServiceReference> filter) {
    Stream<ServiceEndpoint> stream = serviceEndpoints.values().stream();

    // Convert to stream of service references
    Stream<ServiceReference> serviceReferenceStream = stream
        .flatMap(e -> e.serviceRegistrations().stream().map(c -> new ServiceReference(c, e)));

    // Filter by filter
    if (filter != null) {
      serviceReferenceStream = serviceReferenceStream.filter(filter);
    }

    // Collect results
    return serviceReferenceStream.collect(Collectors.toList());
  }
}
