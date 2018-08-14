package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import org.jctools.maps.NonBlockingHashMap;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinServiceRouter implements Router {

  private final Map<String, AtomicInteger> counterByServiceName = new NonBlockingHashMap<>();

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    List<ServiceReference> serviceInstances = serviceRegistry.lookupService(request);
    if (serviceInstances.isEmpty()) {
      return Optional.empty();
    } else if (serviceInstances.size() == 1) {
      return Optional.of(serviceInstances.get(0));
    } else {
      AtomicInteger counter = counterByServiceName.computeIfAbsent(request.qualifier(), or -> new AtomicInteger());
      int index = (counter.incrementAndGet() & Integer.MAX_VALUE) % serviceInstances.size();
      return Optional.of(serviceInstances.get(index));
    }
  }

}
