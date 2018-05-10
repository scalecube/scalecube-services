package io.scalecube.services.routing;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class RouterFactory {
  private static final Logger LOGGER = LoggerFactory.getLogger(RouterFactory.class);

  private static final ConcurrentHashMap<Class<? extends Router>, Router> routers = new ConcurrentHashMap<>();

  private RouterFactory() {
  }

  /**
   * get router instance by a given router class.
   * The class should have a default constructor. otherwise no router can be created
   * 
   * @param routing the type of the Router.
   * @return instance of the Router.
   */
  public static Router getRouter(Class<? extends Router> routing) {
    return routers.computeIfAbsent(routing, RouterFactory::create);
  }

  private static Router create(Class<? extends Router> routing) {
    try {
      return routing.newInstance();
    } catch (Exception ex) {
      LOGGER.error("create router type: {} failed: {}", routing, ex);
      return null;
    }
  }
}
