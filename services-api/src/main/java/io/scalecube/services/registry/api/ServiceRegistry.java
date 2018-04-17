package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Predicate;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface ServiceRegistry {

  List<ServiceInstance> serviceLookup(String serviceName);
  List<ServiceInstance> serviceLookup(Predicate<? super ServiceInstance> filter);

  void registerService(Object serviceObject, Address address);
  void registerService(Object service, Address address, Map<String, String> tags);

  Collection<ServiceInstance> services();



}
