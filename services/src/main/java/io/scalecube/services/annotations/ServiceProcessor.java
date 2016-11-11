package io.scalecube.services.annotations;

import io.scalecube.services.ServiceDefinition;

import java.util.Collection;


public interface ServiceProcessor {

  Collection<Class<?>> extractServiceInterfaces(Object serviceObject);

  ServiceDefinition introspectServiceInterface(Class<?> serviceInterface);

}
