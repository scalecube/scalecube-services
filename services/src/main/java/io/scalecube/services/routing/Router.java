package io.scalecube.services.routing;

import io.scalecube.services.ServiceInstance;
import io.scalecube.transport.Message;

import java.util.Collection;
import java.util.Optional;

public interface Router {

  /**
   * returns service instance if a given request message is applicable.
   */
  Optional<ServiceInstance> route(Message request);

  /**
   * returns all applicable routes.
   */
  Collection<ServiceInstance> routes(Message request);

}
