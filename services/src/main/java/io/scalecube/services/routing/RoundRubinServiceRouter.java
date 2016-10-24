package io.scalecube.services.routing;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;

public class RoundRubinServiceRouter implements Router {

  private final IServiceRegistry serviceRegistry;

  private final ConcurrentMap<ServiceDefinition, AtomicInteger> roundrubin = new ConcurrentHashMap<>();

  public RoundRubinServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public ServiceInstance route(ServiceDefinition serviceDefinition) {

    Collection<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());

    if (!serviceInstances.isEmpty()) {
      AtomicInteger index = roundrubin.computeIfAbsent(serviceDefinition, k -> f());
      if (index.get() > serviceInstances.size() - 1) {
        index.set(0);
      }
      ServiceInstance ref = (ServiceInstance) serviceInstances.stream().toArray()[index.get()];
      index.incrementAndGet();
      return ref;
    } else {
      return null;
    }
  }

  private AtomicInteger f() {
    return new AtomicInteger(0);
  }
}
