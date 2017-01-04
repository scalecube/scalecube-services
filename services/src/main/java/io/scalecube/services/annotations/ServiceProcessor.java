package io.scalecube.services.annotations;

import io.scalecube.services.ServiceDefinition;
import java.lang.reflect.Field;

import java.util.Collection;
import java.util.Set;


public interface ServiceProcessor {

  default Collection<Class<?>> extractServiceInterfaces(Object service){return extractServiceInterfaces(service.getClass());}
  
  Collection<Class<?>> extractServiceInterfaces(Class<?> serviceInterface);

  ServiceDefinition introspectServiceInterface(Class<?> serviceInterface);

  Set<ServiceDefinition> serviceDefinitions(Class<?> serviceInterface);
  
  Collection<Class<?>> extractConstructorInjectables(Class<?> serviceImpl);
  
  Collection<Field> extractMemberInjectables(Class<?> serviceImpl);
  
  boolean isServiceInterface(Class<?> clsType);

}
