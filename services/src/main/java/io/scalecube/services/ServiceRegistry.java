package io.scalecube.services;

import io.scalecube.services.ServicesConfig.Builder.ServiceConfig;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface ServiceRegistry {

  List<ServiceInstance> serviceLookup(String serviceName);

  void registerService(ServiceConfig serviceObject);

  Collection<ServiceInstance> services();
  Collection<ServiceInstance> localServices();

  Optional<ServiceInstance> getLocalInstance(String serviceName, String method);

  void start();

}
