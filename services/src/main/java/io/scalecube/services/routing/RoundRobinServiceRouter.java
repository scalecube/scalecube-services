package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;

import org.jctools.maps.NonBlockingHashMap;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinServiceRouter implements Router {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinServiceRouter.class);

  private final Map<String, AtomicInteger> counterByServiceName = new NonBlockingHashMap<>();

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {

    List<ServiceReference> serviceInstances = serviceRegistry.lookupService(request.qualifier());

    if (serviceInstances.size() > 1) {
      AtomicInteger counter = counterByServiceName
          .computeIfAbsent(request.qualifier(), or -> new AtomicInteger());
      int index = (counter.incrementAndGet() & Integer.MAX_VALUE) % serviceInstances.size();
      return Optional.of(serviceInstances.get(index));
    } else if (serviceInstances.size() == 1) {
      return Optional.of(serviceInstances.get(0));
    } else {
      LOGGER.warn("route selection return null since no service instance was found for {}", request.qualifier());
      return Optional.empty();
    }
  }

  @Override
  public List<ServiceReference> routes(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return serviceRegistry.lookupService(request.qualifier());
  }

}
