package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscoveryContext;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.gateway.Gateway;
import java.util.List;
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
   * Returns service discovery context by id.
   *
   * @param id service discovery id
   * @return service discovery context
   */
  ServiceDiscoveryContext discovery(String id);

  /**
   * All gateways registered on this Microservices node.
   *
   * @return list of gateway
   */
  List<Gateway> gateways();

  /**
   * Return gateway by id.
   *
   * @param id gateway id
   * @return gateway
   */
  Gateway gateway(String id);

  /**
   * Network address current Microservices node.
   *
   * @return network address
   */
  Address serviceAddress();

  /**
   * Unique id of this Microservices node.
   *
   * @return id
   */
  String id();
}
