package io.scalecube.services.routing;

import io.scalecube.services.ServiceEndpoint;
import io.scalecube.services.api.ServiceMessage;

import java.util.Collection;
import java.util.Optional;

public interface Router {

  /**
   * returns service instance if a given request message is applicable.
   */
  Optional<ServiceEndpoint> route(ServiceMessage request);

  /**
   * returns all applicable routes.
   */
  Collection<ServiceEndpoint> routes(ServiceMessage request);

}
