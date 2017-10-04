package io.scalecube.services.routing;

import io.scalecube.services.ServiceHeaders;
import io.scalecube.services.ServiceInstance;
import io.scalecube.services.ServiceRegistry;
import io.scalecube.transport.Message;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collection;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

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

    List<ServiceInstance> serviceInstances = serviceRegistry.serviceLookup(serviceName)
        .stream().filter(instance -> instance.methodExists(request.header(ServiceHeaders.METHOD)))
        .collect(Collectors.toList());

    if (serviceInstances.size() > 1) {
      AtomicInteger counter = counterByServiceName
          .computeIfAbsent(serviceName, or -> new AtomicInteger());
      int index = counter.incrementAndGet() % serviceInstances.size();
      return Optional.of(serviceInstances.get(index));
    } else if (serviceInstances.size() == 1) {
      return Optional.of(serviceInstances.get(0));
    } else {
      LOGGER.warn("route selection return null since no service instance was found for {}", serviceName);
      return Optional.empty();
    }
  }

  @Override
  public Collection<ServiceInstance> routes(Message request) {
    String serviceName = request.header(ServiceHeaders.SERVICE_REQUEST);
    return Collections.unmodifiableCollection(serviceRegistry.serviceLookup(serviceName));
  }

}
