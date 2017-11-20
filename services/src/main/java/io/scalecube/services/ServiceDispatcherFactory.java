package io.scalecube.services;

import io.scalecube.services.metrics.Metrics;
import io.scalecube.services.routing.Router;
import io.scalecube.services.routing.RouterFactory;

import java.time.Duration;

public class ServiceDispatcherFactory {

  private final RouterFactory routerFactory;

  public ServiceDispatcherFactory(ServiceRegistry serviceRegistry) {
    this.routerFactory = new RouterFactory(serviceRegistry);
  }

  public ServiceCall createDispatcher(Class<? extends Router> routerType, Duration timeout, Metrics metrics) {
    return new ServiceCall(routerFactory.getRouter(routerType), timeout, metrics);
  }

}
