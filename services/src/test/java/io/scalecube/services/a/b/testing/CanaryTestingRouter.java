package io.scalecube.services.a.b.testing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.List;
import java.util.Optional;

public class CanaryTestingRouter implements Router {

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    RandomCollection<ServiceReference> weightedRandom = new RandomCollection<>();
    serviceRegistry.lookupService(serviceName)
        .forEach(sr -> weightedRandom.add(Double.valueOf(sr.tags().get("Weight")), sr));
    return Optional.of(weightedRandom.next());
  }

  @Override
  public List<ServiceReference> routes(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return serviceRegistry.lookupService(Messages.qualifierOf(request).getNamespace());
  }

}
