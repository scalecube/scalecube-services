package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.registry.api.ServiceRegistry;

import java.util.List;
import java.util.Optional;

@FunctionalInterface
public interface Router {

  /**
   * returns service instance if a given request message is applicable.
   */
  default Optional<ServiceReference> route(ServiceRegistry serviceRegistry, ServiceMessage request) {
    return routes(serviceRegistry, request).stream().findFirst();
  }

  /**
   * returns all applicable routes.
   */
  List<ServiceReference> routes(ServiceRegistry serviceRegistry, ServiceMessage request);

}
