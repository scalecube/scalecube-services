package io.scalecube.services.routing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  private final ServiceRegistry serviceRegistry;

  public RandomServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceReference> route(ServiceMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    List<ServiceReference> serviceInstances = serviceRegistry.lookupService(serviceName);
    if (!serviceInstances.isEmpty()) {
      int index = ThreadLocalRandom.current().nextInt((serviceInstances.size()));
      return Optional.of(serviceInstances.get(index));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public List<ServiceReference> routes(ServiceMessage request) {
    return serviceRegistry.lookupService(Messages.qualifierOf(request).getNamespace());
  }

}
