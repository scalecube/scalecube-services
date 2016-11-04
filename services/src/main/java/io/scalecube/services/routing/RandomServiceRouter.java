package io.scalecube.services.routing;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  private final IServiceRegistry serviceRegistry;

  public RandomServiceRouter(IServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceInstance> route(ServiceDefinition serviceDefinition) {
    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());
    if (!serviceInstances.isEmpty()) {
      int index = ThreadLocalRandom.current().nextInt((serviceInstances.size()));
      return Optional.of(serviceInstances.get(index));
    } else {
      return Optional.empty();
    }
  }

}
