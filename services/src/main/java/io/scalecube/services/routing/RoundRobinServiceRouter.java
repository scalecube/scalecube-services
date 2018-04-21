package io.scalecube.services.routing;

import io.scalecube.services.Messages;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;

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
  public Optional<ServiceReference> route(ServiceMessage request) {

    String serviceName = Messages.qualifierOf(request).getNamespace();
    String methodName = Messages.qualifierOf(request).getAction();

    System.out.println( serviceRegistry.listServiceReferences());
    List<ServiceReference> serviceInstances =
        serviceRegistry.lookupService(
            sr -> serviceName.equalsIgnoreCase(sr.namespace()) && methodName.equalsIgnoreCase(sr.action()));

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
  public List<ServiceReference> routes(ServiceMessage request) {
    return serviceRegistry.lookupService(Messages.qualifierOf(request).getNamespace());
  }

}
