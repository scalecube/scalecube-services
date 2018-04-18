package io.scalecube.services.a.b.testing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import io.scalecube.services.routing.Router;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;


public class CanaryTestingRouter implements Router {

  private ServiceRegistry serviceRegistry;

  public CanaryTestingRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceEndpoint> route(ServiceMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    RandomCollection<ServiceEndpoint> weightedRandom = new RandomCollection<>();
    serviceRegistry.serviceLookup(serviceName).stream().forEach(instance -> {
      weightedRandom.add(
          Double.valueOf(instance.tags().get("Weight")),
          instance);
    });
    return Optional.of(weightedRandom.next());
  }

  @Override
  public Collection<ServiceEndpoint> routes(ServiceMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    return Collections.unmodifiableCollection(serviceRegistry.serviceLookup(serviceName));
  }
}
