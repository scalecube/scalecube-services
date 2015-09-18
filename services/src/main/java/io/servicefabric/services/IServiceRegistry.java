package io.servicefabric.services;

import com.google.common.base.Predicate;

import java.util.Collection;
import java.util.Map;
import java.util.Set;

import io.servicefabric.services.annotations.ServiceInstance;

/**
 * The Interface IServiceRegistry. provides possibility to register/unregister services in the system and make services
 * lookup
 */
public interface IServiceRegistry {

  /**
   * Register service in the system with namespace and service properties, bind namespace to namespaceGroup if available
   * for this service
   * 
   * @param namespace service namespace
   * @throws IllegalArgumentException if namespace parameter is null
   */
  void registerService(Object serviceObject);

  void registerService(ServiceInstance serviceInstance);

  /**
   * Return collection of service references that related to given namespace
   * 
   * @param namespace service namespace
   * @return Collection of service references, related to namespace, if not found return empty collection.
   * @throws IllegalArgumentException if namespace parameter is null
   */
  Collection<ServiceInstance> serviceLookup(String namespace);

}
