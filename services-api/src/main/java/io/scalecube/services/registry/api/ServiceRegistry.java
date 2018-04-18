package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceReference;

import java.util.Collection;
import java.util.List;
import java.util.function.Predicate;

/**
 * Service registry interface provides API to register/unregister services in the system and make services lookup by
 * service result.
 */
public interface ServiceRegistry {

  List<ServiceReference> serviceLookup(String serviceName);

  List<ServiceReference> serviceLookup(Predicate<? super ServiceReference> filter);

  Collection<ServiceReference> listServices();

  void registerService(ServiceReference serviceReference);

  void unregisterService(ServiceReference serviceReference);
}
