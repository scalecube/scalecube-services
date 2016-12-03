package io.scalecube.services.annotations;

import io.scalecube.services.ServiceDefinition;

import java.util.Collection;
import java.util.Set;


public interface ServiceProcessor {

  Collection<Class<?>> extractServiceInterfaces(Object serviceObject);

  ServiceDefinition introspectServiceInterface(Class<?> serviceInterface);

  Set<ServiceDefinition> serviceDefinitions(Object service);

}
