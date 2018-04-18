package io.scalecube.services.registry;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final ConcurrentMap<String, ServiceEndpoint> serviceReferences = new ConcurrentHashMap<>();

  @Override
  public void registerService(ServiceEndpoint serviceEndpoint) {
    serviceReferences.putIfAbsent(serviceEndpoint.endpointId(), serviceEndpoint);
  }

  @Override
  public void unregisterService(ServiceEndpoint serviceEndpoint) {
    serviceReferences.remove(serviceEndpoint.endpointId());
  }

  @Override
  public Collection<ServiceEndpoint> listServices() {
    return serviceReferences.values();
  }

  @Override
  public List<ServiceEndpoint> serviceLookup(String serviceName) {
    return serviceReferences.values().stream()
        .filter(entry -> entry.equals(serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public List<ServiceEndpoint> serviceLookup(Predicate<? super ServiceEndpoint> filter) {
    return serviceReferences.values().stream()
        .filter(filter)
        .collect(Collectors.toList());
  }
}
