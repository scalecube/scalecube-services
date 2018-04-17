package io.scalecube.services.routing;

import io.scalecube.services.registry.api.ServiceRegistry;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class RouterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RouterFactory.class);

  private final ConcurrentHashMap<Class<? extends Router>, Router> routers = new ConcurrentHashMap<>();
  private final ServiceRegistry serviceRegistry;

  public RouterFactory(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  /**
   * get router instance by a given router class.
   * 
   * @param routing the type of the Router.
   * @return instance of the Router.
   */
  public Router getRouter(Class<? extends Router> routing) {
    return routers.computeIfAbsent(routing, this::create);
  }

  private Router create(Class<? extends Router> routing) {
    try {
      return routing.getDeclaredConstructor(ServiceRegistry.class).newInstance(serviceRegistry);
    } catch (Exception ex) {
      LOGGER.error("create router type: {} failed: {}", routing, ex);
      return null;
    }
  }
}
