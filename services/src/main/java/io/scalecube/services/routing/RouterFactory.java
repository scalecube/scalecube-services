package io.scalecube.services.routing;

import java.util.concurrent.ConcurrentHashMap;

import io.scalecube.services.ServiceDefinition;
import io.scalecube.services.ServiceRegistry;

public class RouterFactory {

  ConcurrentHashMap<Class<?>, RouteSelectionStrategy> routers = new ConcurrentHashMap<>();
  private final ServiceRegistry serviceRegistry;

  public RouterFactory(ServiceRegistry serviceRegistry) {
    this.serviceRegistry = serviceRegistry;
  }

  public RouteSelectionStrategy getRouter(ServiceDefinition def) {
    try {
      return routers.computeIfAbsent(def.routing(), k -> create(k));
    } catch (Exception e) {
      return null;
    }
  }

  private RouteSelectionStrategy create(Class<?> clazz) {
    try {
      return (RouteSelectionStrategy) clazz.getDeclaredConstructor(ServiceRegistry.class).newInstance(serviceRegistry);
    } catch (Exception e) {
      return null;
    }
  }
}
