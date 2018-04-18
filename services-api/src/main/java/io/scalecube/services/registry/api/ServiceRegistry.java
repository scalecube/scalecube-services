package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface ServiceRegistry {

  List<ServiceEndpoint> serviceLookup(String serviceName);

  List<ServiceEndpoint> serviceLookup(Predicate<? super ServiceEndpoint> filter);

  Collection<ServiceEndpoint> listServices();

  void registerService(ServiceEndpoint serviceEndpoint);

  void unregisterService(ServiceEndpoint serviceEndpoint);
}
