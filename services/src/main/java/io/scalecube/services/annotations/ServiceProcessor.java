package io.scalecube.services.annotations;

import io.scalecube.services.ServiceDefinition;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;


public interface ServiceProcessor {

  Collection<Class<?>> extractServiceInterfaces(Object serviceObject);

  ConcurrentMap<String, ServiceDefinition> introspectServiceInterface(Class<?> serviceInterface);

}
