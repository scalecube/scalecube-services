package io.scalecube.services.registry.api;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import java.util.List;

/**
 * Service registry interface provides API to register/unregister services in the system and make
 * services lookup by service result.
 */
public interface ServiceRegistry {

  List<ServiceEndpoint> listServiceEndpoints();

  List<ServiceReference> listServiceReferences();

  List<ServiceReference> lookupService(ServiceMessage request);

  boolean registerService(ServiceEndpoint serviceEndpoint);

  ServiceEndpoint unregisterService(String endpointId);
}
