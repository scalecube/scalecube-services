package io.scalecube.services;

import java.util.List;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface IServiceRegistry {

  void registerService(Object serviceObject, String[] tags);

  void unregisterService(Object serviceObject);

  List<ServiceInstance> serviceLookup(String serviceName);

  ServiceInstance serviceInstance(ServiceReference reference);

  ServiceInstance getLocalInstance(String serviceName);


}
