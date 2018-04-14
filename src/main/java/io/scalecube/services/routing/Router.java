package io.scalecube.services.routing;

import io.scalecube.services.ServiceInstance;
import io.scalecube.services.transport.api.ServiceMessage;

import java.util.Collection;
import java.util.Optional;

public interface Router {

  /**
   * returns service instance if a given request message is applicable.
   */
  <T> Optional<ServiceInstance> route(ServiceMessage request);

  /**
   * returns all applicable routes.
   */
  Collection<ServiceInstance> routes(ServiceMessage request);

}
