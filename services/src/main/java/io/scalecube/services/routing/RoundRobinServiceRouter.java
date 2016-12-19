package io.scalecube.services.routing;

import io.scalecube.services.ServiceHeaders;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;

public class RoundRobinServiceRouter implements Router {

  private static final Logger LOGGER = LoggerFactory.getLogger(RoundRobinServiceRouter.class);

  private final ServiceRegistry serviceRegistry;
  private final ConcurrentMap<String, AtomicInteger> counterByServiceName = new ConcurrentHashMap<>();

  public RoundRobinServiceRouter(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  @Override
  public Optional<ServiceInstance> route(Message request) {

    String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceName);
    if (!serviceInstances.isEmpty()) {
      AtomicInteger counter = counterByServiceName
          .computeIfAbsent(serviceName, or -> new AtomicInteger());
      int index = counter.incrementAndGet() % serviceInstances.size();
      return Optional.of(serviceInstances.get(index));
    } else {
      LOGGER.warn("route selection return null since no service instance was found for {}", serviceName);
      return Optional.empty();
    }
  }

}
