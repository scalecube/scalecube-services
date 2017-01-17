package io.scalecube.services;

import io.scalecube.services.routing.Router;

import java.time.Duration;


public class ProxyDefinition {
  
  private final Class<?> serviceInterface;
  private final Class<? extends Router> router;
  private final Duration duration;

  public ProxyDefinition(Class<?> serviceInterface) {
    this.serviceInterface = serviceInterface;
    this.router = null;
    this.duration = Duration.ZERO;
  }
  
  public ProxyDefinition(Class<?> serviceInterface, Class<? extends Router> router, Duration duration) {
    this.serviceInterface = serviceInterface;
    this.router = router;
    this.duration = duration;
  }

  public Class<?> getServiceInterface() {
    return serviceInterface;
  }

  public Class<? extends Router> getRouter() {
    return router;
  }

  public Duration getDuration() {
    return duration;
  }
  
  
}
