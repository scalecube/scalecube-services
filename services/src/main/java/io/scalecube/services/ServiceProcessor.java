package io.scalecube.services;

import java.util.Collection;

/**
 * @author Anton Kharenko
 */
public interface ServiceProcessor {

  Collection<Class<?>> extractServiceInterfaces(Object serviceObject);

  ServiceDefinition introspectServiceInterface(Class<?> serviceInterface);

}
