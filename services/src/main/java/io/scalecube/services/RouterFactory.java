package io.scalecube.services;

import java.util.concurrent.ConcurrentHashMap;

import io.scalecube.services.routing.Router;

public class RouterFactory {

  ConcurrentHashMap<Class<?>, Router> routers = new ConcurrentHashMap<>();
  private final ServiceRegistry serviceRegistry;
  
  public RouterFactory(ServiceRegistry serviceRegistry){
    this.serviceRegistry = serviceRegistry;
  }
  
  public Router getRouter(ServiceDefinition def){
    try {
      return routers.computeIfAbsent( def.routing(), k -> create(k));
      } catch (Exception e) {
    }
    return null;
  }
  
  private Router create(Class clazz) {
    try {
      return (Router) clazz.getDeclaredConstructor(ServiceRegistry.class).newInstance(serviceRegistry);
    } catch (Exception e) {
      return null;
    } 
  }
}
