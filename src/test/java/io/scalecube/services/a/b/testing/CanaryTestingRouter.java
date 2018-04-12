package io.scalecube.services.a.b.testing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.services.routing.Router;
import io.scalecube.streams.StreamMessage;

import java.util.Collection;
import java.util.Collections;
import java.util.Optional;


public class CanaryTestingRouter implements Router {

  private ServiceRegistry serviceRegistry;

  public CanaryTestingRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceInstance> route(StreamMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    RandomCollection<ServiceInstance> weightedRandom = new RandomCollection<>();
    serviceRegistry.serviceLookup(serviceName).stream().forEach(instance -> {
      weightedRandom.add(
          Double.valueOf(instance.tags().get("Weight")),
          instance);
    });
    return Optional.of(weightedRandom.next());
  }

  @Override
  public Collection<ServiceInstance> routes(StreamMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    return Collections.unmodifiableCollection(serviceRegistry.serviceLookup(serviceName));
  }
}
