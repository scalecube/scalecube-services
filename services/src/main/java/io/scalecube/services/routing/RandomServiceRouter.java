package io.scalecube.services.routing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.streams.StreamMessage;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ThreadLocalRandom;

public class RandomServiceRouter implements Router {

  private final ServiceRegistry serviceRegistry;

  public RandomServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceInstance> route(StreamMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceName);
    if (!serviceInstances.isEmpty()) {
      int index = ThreadLocalRandom.current().nextInt((serviceInstances.size()));
      return Optional.of(serviceInstances.get(index));
    } else {
      return Optional.empty();
    }
  }

  @Override
  public Collection<ServiceInstance> routes(StreamMessage request) {
    String serviceName = Messages.qualifierOf(request).getNamespace();
    return Collections.unmodifiableCollection(serviceRegistry.serviceLookup(serviceName));
  }

}
