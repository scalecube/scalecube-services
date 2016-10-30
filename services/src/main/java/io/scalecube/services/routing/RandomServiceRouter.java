package io.scalecube.services.routing;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  private final IServiceRegistry serviceRegistry;

  public RandomServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  private int random(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max);
  }

  @Override
  public ServiceInstance route(ServiceDefinition serviceDefinition) {

    Collection<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());
    if (serviceInstances.isEmpty()) {
      return null;
    } else {
      int rnd = random(0, serviceInstances.size());
      return (ServiceInstance) serviceInstances.toArray()[rnd];
    }
  }

}
