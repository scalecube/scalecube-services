package io.scalecube.services.routing;

import java.util.concurrent.ConcurrentHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import io.scalecube.services.ServiceRegistry;

public class RouterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RouterFactory.class);
  
  ConcurrentHashMap<Class<? extends Router>, Router> routers = new ConcurrentHashMap<>();
  private final ServiceRegistry serviceRegistry;

  public RouterFactory(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public Router getRouter(Class<? extends Router> routing) {
    try {
      return routers.computeIfAbsent(routing, k -> create(k));
    } catch (Exception ex) {
      LOGGER.error("get router type: {} failed: {}",routing,ex);
    }
    return null;
  }

  private Router create(Class<? extends Router> routing) {
    try {
      return (Router) routing.getDeclaredConstructor(ServiceRegistry.class).newInstance(serviceRegistry);
    } catch (Exception ex) {
      LOGGER.error("create router type: {} failed: {}",routing,ex);
      return null;
    }
  }
}
