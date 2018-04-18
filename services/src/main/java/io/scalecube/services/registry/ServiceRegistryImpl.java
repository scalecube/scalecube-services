package io.scalecube.services.registry;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final ConcurrentMap<String, ServiceReference> serviceReferences = new ConcurrentHashMap<>();

  @Override
  public void registerService(ServiceReference serviceReference) {
    serviceReferences.putIfAbsent(serviceReference.endpointId(), serviceReference);
  }

  @Override
  public void unregisterService(ServiceReference serviceReference) {
    serviceReferences.remove(serviceReference.endpointId());
  }

  @Override
  public Collection<ServiceReference> listServices() {
    return serviceReferences.values();
  }

  @Override
  public List<ServiceReference> serviceLookup(String serviceName) {
    return serviceReferences.values().stream()
        .filter(entry -> entry.equals(serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList());
  }

  @Override
  public List<ServiceReference> serviceLookup(Predicate<? super ServiceReference> filter) {
    return serviceReferences.values().stream()
        .filter(filter)
        .collect(Collectors.toList());
  }
}
