package io.scalecube.services;

import java.util.Collection;

/**
 * Service registry interface provides API to register/unregister services in the system and make services
 * lookup by service name.
 *
 * @author Anton Kharenko
 */
public interface IServiceRegistry {

  void registerService(Object serviceObject);

  void unregisterService(Object serviceObject);

  Collection<ServiceReference> serviceLookup(String serviceName);

  ServiceInstance serviceInstance(ServiceReference serviceReference);

  ServiceInstance localServiceInstance(String serviceName);

}
