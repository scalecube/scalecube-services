package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    List<ServiceReference> serviceInstances = serviceRegistry.lookupService(request.qualifier());
    if (!serviceInstances.isEmpty()) {
      int index = ThreadLocalRandom.current().nextInt((serviceInstances.size()));
      return Optional.of(serviceInstances.get(index));
    } else {
      return Optional.empty();
    }
  }

}
