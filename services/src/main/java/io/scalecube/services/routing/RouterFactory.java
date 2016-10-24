package io.scalecube.services.routing;

import java.util.concurrent.ConcurrentHashMap;

import io.scalecube.services.ServiceRegistry;

public class RouterFactory {

  ConcurrentHashMap<Class<? extends Router>, Router> routers = new ConcurrentHashMap<>();
  private final ServiceRegistry serviceRegistry;
  
  public RouterFactory(ServiceRegistry serviceRegistry){
    this.serviceRegistry = serviceRegistry;
  }
  
  public Router getRouter(Class<? extends Router> routing){
    try {
      return routers.computeIfAbsent( routing , k -> create(k));
      } catch (Exception e) {
    }
    return null;
  }
  
  private Router create(Class<? extends Router> clazz) {
    try {
      return (Router) clazz.getDeclaredConstructor(ServiceRegistry.class).newInstance(serviceRegistry);
    } catch (Exception e) {
      return null;
    } 
  }
}
