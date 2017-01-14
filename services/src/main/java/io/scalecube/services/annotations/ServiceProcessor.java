package io.scalecube.services.annotations;

import io.scalecube.services.ProxyDefinition;
import io.scalecube.services.ServiceDefinition;

import java.lang.reflect.Constructor;
import java.lang.reflect.Field;

import java.util.Collection;
import java.util.Set;

public interface ServiceProcessor {

  Collection<Class<?>> extractServiceInterfaces(Object service);

  Collection<Class<?>> extractServiceInterfaces(Class<?> serviceInterface);

  ServiceDefinition introspectServiceInterface(Class<?> serviceInterface);

  Set<ServiceDefinition> serviceDefinitions(Class<?> serviceInterface);

  Collection<Class<?>> extractInjectableParameterFromConstructor(Constructor<?> constructor);
  
  Collection<ProxyDefinition> extractServiceProxyFromConstructor(Constructor<?> constructor);
  
  Constructor<?> extractConstructorInjectables(Class<?> serviceImpl);

  Collection<Field> extractMemberInjectables(Class<?> serviceImpl);

  Collection<ProxyDefinition> extractServiceProxyFromMembers(Collection<Field> fields);
  
  boolean isServiceInterface(Class<?> clsType);

}
