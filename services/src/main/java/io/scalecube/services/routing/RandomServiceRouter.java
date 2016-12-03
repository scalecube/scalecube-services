package io.scalecube.services.routing;

import io.scalecube.services.ServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  private final ServiceRegistry serviceRegistry;

  public RandomServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceInstance> route(ServiceDefinition serviceDefinition) {
    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.serviceName());
    if (!serviceInstances.isEmpty()) {
      int index = ThreadLocalRandom.current().nextInt((serviceInstances.size()));
      return Optional.of(serviceInstances.get(index));
    } else {
      return Optional.empty();
    }
  }
}
