package io.scalecube.services;

import java.util.Collection;
import java.util.List;

/**
 * Service registry interface provides API to register/unregister services in the system and make services
 * lookup by service name.
 */
public interface IServiceRegistry {

  void registerService(Object serviceObject);

  void unregisterService(Object serviceObject);

  Collection<ServiceInstance> serviceLookup(String serviceName);

  ServiceInstance serviceInstance(ServiceReference reference);

  List<RemoteServiceInstance> findRemoteInstance(String serviceName);

  ServiceInstance getLocalInstance(String serviceName);


}
