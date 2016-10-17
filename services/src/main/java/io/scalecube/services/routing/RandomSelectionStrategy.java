package io.scalecube.services.routing;

import java.util.Collection;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

import io.scalecube.services.IServiceRegistry;
import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;

public class RandomSelectionStrategy implements RouteSelectionStrategy{

  private final IServiceRegistry serviceRegistry;

  public RandomSelectionStrategy(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  private int random(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max);
  }

  @Override
  public Optional<ServiceInstance> route(ServiceDefinition serviceDefinition) {
    
    Collection<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceDefinition.qualifier());
    if (serviceInstances.isEmpty()) {
      return null;
    } else {
      int rnd = random(0, serviceInstances.size());
      return Optional.ofNullable( ( (ServiceInstance) serviceInstances.toArray()[rnd]));
    }
  }
  
}
