package io.scalecube.services.a.b.testing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.List;
import java.util.Optional;


public class CanaryTestingRouter implements Router {

  private ServiceRegistry serviceRegistry;

  public CanaryTestingRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceReference> route(ServiceMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    RandomCollection<ServiceReference> weightedRandom = new RandomCollection<>();
    serviceRegistry.lookupService(serviceName).forEach(serviceReference -> {
      weightedRandom.add(
          Double.valueOf(serviceReference.tags().get("Weight")),
          serviceReference);
    });
    return Optional.of(weightedRandom.next());
  }

  @Override
  public List<ServiceReference> routes(ServiceMessage request) {
    return serviceRegistry.lookupService(Messages.qualifierOf(request).getNamespace());
  }
}
