package io.scalecube.services.routing;

import com.google.common.base.Throwables;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.ConcurrentHashMap;

public class Routers {
  private static final Logger LOGGER = LoggerFactory.getLogger(Routers.class);

  private static final ConcurrentHashMap<Class<? extends Router>, Router> routers = new ConcurrentHashMap<>();

  private Routers() {}

  /**
   * Get router instance by a given router class. The class should have a default constructor. Otherwise no router can
   * be created
   * 
   * @param routerType the type of the Router.
   * @return instance of the Router.
   */
  public static Router getRouter(Class<? extends Router> routerType) {
    return routers.computeIfAbsent(routerType, Routers::create);
  }

  private static Router create(Class<? extends Router> routerType) {
    try {
      return routerType.newInstance();
    } catch (Exception ex) {
      LOGGER.error("Create router type: {} failed: {}", routerType, ex);
      throw Throwables.propagate(ex);
    }
  }
}
