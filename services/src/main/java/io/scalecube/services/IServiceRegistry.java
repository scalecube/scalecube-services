package io.scalecube.services;

import java.util.Collection;

/**
 * Service registry interface provides API to register/unregister services in the system and make services
 * lookup by service name.
 */
public interface IServiceRegistry {

  void registerService(Object serviceObject);

  void unregisterService(Object serviceObject);

  Collection<ServiceReference> serviceLookup(String serviceName);

  ServiceInstance serviceInstance(ServiceReference reference);

  ServiceInstance localServiceInstance(String serviceName);

  ServiceInstance findRemoteInstance(String serviceName);


}
