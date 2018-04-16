package io.scalecube.services;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;

import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.function.Predicate;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface ServiceRegistry {

  List<ServiceInstance> serviceLookup(String serviceName);
  List<ServiceInstance> serviceLookup(Predicate<? super ServiceInstance> filter);

  void registerService(ServiceConfig serviceObject);

  Collection<ServiceInstance> services();

  Optional<ServiceInstance> getLocalInstance(String serviceName, String method);

  void start();


}
