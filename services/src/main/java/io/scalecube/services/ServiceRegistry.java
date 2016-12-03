package io.scalecube.services;

import io.scalecube.cluster.ICluster;

import java.util.Collection;
import java.util.List;
import java.util.Optional;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface ServiceRegistry {

  void registerService(Object serviceObject, Tag[] tags);

  void unregisterService(Object serviceObject);

  List<ServiceInstance> serviceLookup(String serviceName);

  Optional<ServiceInstance> getLocalInstance(String serviceName, String method);

  Collection<ServiceInstance> services();

  ICluster cluster();

}
