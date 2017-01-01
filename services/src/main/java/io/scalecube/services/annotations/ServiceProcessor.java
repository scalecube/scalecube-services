package io.scalecube.services.annotations;

import io.scalecube.services.ServiceDefinition;

import java.util.Collection;
import java.util.Set;


public interface ServiceProcessor {

  default Collection<Class<?>> extractServiceInterfaces(Object service){return extractServiceInterfaces(service.getClass());}
  
  Collection<Class<?>> extractServiceInterfaces(Class<?> serviceInterface);

  ServiceDefinition introspectServiceInterface(Class<?> serviceInterface);

  Set<ServiceDefinition> serviceDefinitions(Class<?> serviceInterface);
  
  Collection<Class<?>> extractInjectables(Class<?> serviceImpl);
  

}
