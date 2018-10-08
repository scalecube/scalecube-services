package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinServiceRouter implements Router {

  private final ThreadLocal<Map<String, AtomicInteger>> counterByServiceName =
      ThreadLocal.withInitial(HashMap::new);

  @Override
  public Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    List<ServiceReference> serviceInstances = serviceRegistry.lookupService(request);
    if (serviceInstances.isEmpty()) {
      return Optional.empty();
    } else if (serviceInstances.size() == 1) {
      return Optional.of(serviceInstances.get(0));
    } else {
      Map<String, AtomicInteger> map = counterByServiceName.get();
      AtomicInteger counter = map.computeIfAbsent(request.qualifier(), or -> new AtomicInteger());
      int index = (counter.incrementAndGet() & Integer.MAX_VALUE) % serviceInstances.size();
      return Optional.of(serviceInstances.get(index));
    }
  }
}
