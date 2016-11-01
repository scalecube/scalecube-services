package io.scalecube.services.routing;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinServiceRouter implements Router {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinServiceRouter.class);

  private final IServiceRegistry serviceRegistry;
  private final ConcurrentMap<String, AtomicInteger> counterByServiceName = new ConcurrentHashMap<>();

  public RoundRobinServiceRouter(IServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public ServiceInstance route(ServiceDefinition serviceDefinition) {
    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());
    if (!serviceInstances.isEmpty()) {
      AtomicInteger counter = counterByServiceName
          .computeIfAbsent(serviceDefinition.qualifier(), or -> new AtomicInteger());
      int index = counter.incrementAndGet() % serviceInstances.size();
      return serviceInstances.get(index);
    } else {
      LOGGER.warn("route selection return null since no service instance was found for {}", serviceDefinition);
      return null; // TODO AK: Return Optional<ServiceInstance> instead
    }
  }

}
