package io.scalecube.services.routing;

import io.scalecube.services.ServiceReference;
import io.scalecube.services.api.ServiceMessage;

import java.util.List;
import java.util.Optional;

public interface Router {

  /**
   * returns service instance if a given request message is applicable.
   */
  Optional<ServiceReference> route(ServiceMessage request);

  /**
   * returns all applicable routes.
   */
  List<ServiceReference> routes(ServiceMessage request);

}
