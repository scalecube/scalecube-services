package io.scalecube.services.routing;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRubinServiceRouter implements Router {
  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRubinServiceRouter.class);

  private final IServiceRegistry serviceRegistry;

  private final ConcurrentMap<ServiceDefinition, AtomicInteger> roundrubin = new ConcurrentHashMap<>();

  public RoundRubinServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public ServiceInstance route(ServiceDefinition serviceDefinition) {

    Collection<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());

    if (!serviceInstances.isEmpty()) {
      AtomicInteger index = roundrubin.computeIfAbsent(serviceDefinition, or -> compute());
      if (index.get() > serviceInstances.size() - 1) {
        index.set(0);
      }
      ServiceInstance ref = (ServiceInstance) serviceInstances.stream().toArray()[index.get()];
      index.incrementAndGet();
      return ref;
    } else {
      LOGGER.warn("route selection return null since no service instance was found for {}", serviceDefinition);
      return null;
    }
  }

  private AtomicInteger compute() {
    return new AtomicInteger(0);
  }
}
