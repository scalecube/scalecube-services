package io.scalecube.services.annotations;

import java.util.Collection;
import java.util.concurrent.ConcurrentMap;

import io.scalecube.services.ServiceDefinition;


public interface ServiceProcessor {

  Collection<Class<?>> extractServiceInterfaces(Object serviceObject);

  ConcurrentMap<String, ServiceDefinition> introspectServiceInterface(Class<?> serviceInterface);

}
