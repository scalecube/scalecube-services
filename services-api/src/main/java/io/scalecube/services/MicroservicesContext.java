package io.scalecube.services;

import io.scalecube.services.discovery.api.ServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import reactor.core.publisher.Flux;

/**
 * Context of Scalecube node. Used in {@link ServiceFactory}.
 *
 * @see ServiceFactory
 */
public interface MicroservicesContext {

  /**
   * Service endpoint of current Scalecube node.
   *
   * @return id
   */
  ServiceEndpoint serviceEndpoint();

  /**
   * Used for remote service call.
   *
   * @return new instance service call
   * @see ServiceCall
   */
  ServiceCall serviceCall();

  /**
   * Flux of service discovery events.
   *
   * @return service discovery
   * @see ServiceDiscovery
   * @see ServiceDiscoveryEvent
   */
  Flux<ServiceDiscoveryEvent> listenDiscoveryEvents();
}
