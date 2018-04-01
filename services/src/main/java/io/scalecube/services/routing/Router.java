package io.scalecube.services.routing;

import io.scalecube.services.ServiceInstance;
import io.scalecube.streams.StreamMessage;

import java.util.Collection;
import java.util.Optional;

public interface Router {

  /**
   * returns service instance if a given request message is applicable.
   */
  Optional<ServiceInstance> route(StreamMessage request);

  /**
   * returns all applicable routes.
   */
  Collection<ServiceInstance> routes(StreamMessage request);

}
