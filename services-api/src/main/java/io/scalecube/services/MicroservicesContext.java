package io.scalecube.services;

import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import reactor.core.publisher.Flux;

/**
 * Context of Microservices node. Contain all public API of Microservices node, include gateways,
 * service discoveries, etc.
 *
 * <p>Can be used in user services.
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
   * Function to subscribe and listen on {@code ServiceDiscoveryEvent} events.
   *
   * @return stream of {@code ServiceDiscoveryEvent} events
   */
  Flux<ServiceDiscoveryEvent> listenDiscovery();

  /**
   * Function to subscribe and listen on {@code ServiceDiscoveryEvent} events by service discovery
   * id.
   *
   * @param id service discovery id
   * @return service discovery context
   */
  Flux<ServiceDiscoveryEvent> listenDiscovery(String id);
}
