package io.scalecube.services;

import io.scalecube.services.discovery.api.ServiceDiscovery;

/**
 * Context of Scalecube node. Used in {@link ServiceFactory}. Compared to MicroservicesContext, it
 * gives access to ServiceDiscovery.
 *
 * @see ServiceFactory
 */
public interface ExtendedMicroservicesContext extends MicroservicesContext {

  /**
   * Service discovery for services localed in other nodes.
   *
   * @return service discovery
   * @see ServiceDiscovery
   */
  ServiceDiscovery serviceDiscovery();
}
