package io.scalecube.services.routing;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;

import java.util.List;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  private final IServiceRegistry serviceRegistry;

  public RandomServiceRouter(IServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public ServiceInstance route(ServiceDefinition serviceDefinition) {
    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());
    if (!serviceInstances.isEmpty()) {
      int index = ThreadLocalRandom.current().nextInt((serviceInstances.size()));
      return serviceInstances.get(index);
    } else {
      return null; // TODO AK: Return Optional<ServiceInstance> instead
    }
  }

}
