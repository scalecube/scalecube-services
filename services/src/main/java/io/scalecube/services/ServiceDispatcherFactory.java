package io.scalecube.services;

import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;
import io.scalecube.services.routing.Routing;

import java.time.Duration;

public class ServiceDispatcherFactory {

  private final ServiceRegistry serviceRegistry;

  public ServiceDispatcherFactory(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public ServiceCall createDispatcher(Routing routing, Duration timeout, Metrics metrics) {
    return new ServiceCall(routing, serviceRegistry, timeout, metrics);
  }

}
