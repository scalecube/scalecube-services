package io.scalecube.services.registry;

import static java.util.Objects.requireNonNull;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.transport.Address;

import com.google.common.collect.Maps;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.function.Predicate;
import java.util.stream.Collectors;

public class ServiceRegistryImpl implements ServiceRegistry {

  private final ConcurrentMap<String, ServiceInstance> serviceInstances = new ConcurrentHashMap<>();

  @Override
  public void registerService(Object serviceObject,Address address) {
    this.registerService(serviceObject,address,null);
  }
  
  /**
   * register a service instance at the cluster.
   * 
   * @param tags
   * @param address
   */
  @Override
  public void registerService(Object service, Address address, Map<String, String> tags) {
    requireNonNull(service != null, "Service object can't be null.");
    Collection<Class<?>> serviceInterfaces = Reflect.serviceInterfaces(service);

    serviceInterfaces.forEach(serviceInterface -> {
      // Process service interface

      ServiceInstance serviceInstance =
          new ServiceInstance(serviceInterface,
              Reflect.serviceName(serviceInterface),
              Reflect.serviceMethods(serviceInterface).keySet(),
              tags,
              address,
              true);

      serviceInstances.putIfAbsent(Reflect.serviceName(serviceInterface), serviceInstance);

    });
  }

  @Override
  public List<ServiceInstance> serviceLookup(final String serviceName) {
    requireNonNull(serviceName != null, "Service name can't be null");

    return Collections.unmodifiableList(serviceInstances.entrySet().stream()
        .filter(entry -> entry.equals(serviceName))
        .map(Map.Entry::getValue)
        .collect(Collectors.toList()));
  }

  @Override
  public List<ServiceInstance> serviceLookup(Predicate<? super ServiceInstance> filter) {
    requireNonNull(filter != null, "Filter can't be null");
    return Collections.unmodifiableList(serviceInstances.values().stream()
        .filter(filter)
        .collect(Collectors.toList()));
  }

  public Collection<ServiceInstance> services() {
    return Collections.unmodifiableCollection(serviceInstances.values());
  }

  

}
