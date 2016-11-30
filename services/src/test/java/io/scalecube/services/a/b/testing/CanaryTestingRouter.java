package io.scalecube.services.a.b.testing;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.Optional;

public class CanaryTestingRouter implements Router {

  private ServiceRegistry serviceRegistry;

  public CanaryTestingRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceInstance> route(ServiceDefinition serviceDefinition) {
    RandomCollection<ServiceInstance> weightedRandom = new RandomCollection<>();    
    serviceRegistry.serviceLookup(serviceDefinition.serviceName()).stream().forEach(instance->{
      weightedRandom.add(
          Double.valueOf(instance.tags().get("Weight")), 
          instance);
    });
    return Optional.of(weightedRandom.next());
  }
}
