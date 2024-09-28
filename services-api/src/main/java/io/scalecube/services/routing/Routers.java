package io.scalecube.services.routing;

import java.util.concurrent.ConcurrentHashMap;
import reactor.core.Exceptions;

public final class Routers {

  private static final ConcurrentHashMap<Class<? extends Router>, Router> routers =
      new ConcurrentHashMap<>();

  private Routers() {}

  /**
   * Get router instance by a given router class. The class should have a default constructor.
   * Otherwise no router can be created
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
      throw Exceptions.propagate(ex);
    }
  }
}
