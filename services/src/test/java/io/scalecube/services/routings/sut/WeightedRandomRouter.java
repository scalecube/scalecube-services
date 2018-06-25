package io.scalecube.services.routings.sut;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.Optional;

public class WeightedRandomRouter implements Router {

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    RandomCollection<ServiceReference> weightedRandom = new RandomCollection<>();
    serviceRegistry.lookupService(request.qualifier())
        .forEach(sr -> weightedRandom.add(Double.valueOf(sr.tags().get("Weight")), sr));
    return Optional.of(weightedRandom.next());
  }

}
