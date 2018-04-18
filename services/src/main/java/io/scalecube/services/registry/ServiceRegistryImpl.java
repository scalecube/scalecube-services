package io.scalecube.services.registry;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final ConcurrentMap<String, ServiceReference> serviceInstances = new ConcurrentHashMap<>();

  @Override
  public void registerService(Object serviceObject,Address address) {
    this.registerService(serviceObject,address,null);
  }
  
  /**
   * register a service instance at the cluster.
   * 
   * @param tags
   * @param address
   * @param contentType 
   */
  @Override
  public void registerService(Object service, Address address, Map<String, String> tags) {
    requireNonNull(service != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = Reflect.serviceInterfaces(service);

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface

      ServiceReference serviceInstance =
          new ServiceReference(
              memberId,
              address,
              serviceRegistrations);

      serviceInstances.putIfAbsent(Reflect.serviceName(serviceInterface), serviceInstance);

    });
  }

  @Override
  public List<ServiceReference> serviceLookup(final String serviceName) {
    requireNonNull(serviceName != null, "Service name can't be null");

    return Collections.unmodifiableList(serviceInstances.entrySet().stream()
        .filter(entry -> entry.equals(serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList()));
  }

  @Override
  public List<ServiceReference> serviceLookup(Predicate<? super ServiceReference> filter) {
    requireNonNull(filter != null, "Filter can't be null");
    return Collections.unmodifiableList(serviceInstances.values().stream()
        .filter(filter)
        .collect(Collectors.toList()));
  }

  public Collection<ServiceReference> services() {
    return Collections.unmodifiableCollection(serviceInstances.values());
  }

  

}
