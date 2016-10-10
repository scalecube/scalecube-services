package io.scalecube.services;

import java.util.Collection;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements IRouter{

  private final IServiceRegistry serviceRegistry;

  public RandomServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public ServiceInstance route(String serviceName) {
    Collection<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceName);
    if (serviceInstances.isEmpty()) {
      return null;
    } else {
      int rnd = random(0, serviceInstances.size());
      return (ServiceInstance) serviceInstances.toArray()[rnd];
    }
  }

  private int random(int min, int max) {
    return ThreadLocalRandom.current().nextInt(min, max);
  }
  
}
