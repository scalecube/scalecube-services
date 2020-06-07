package io.scalecube.services;

import io.scalecube.net.Address;
import io.scalecube.services.discovery.api.ServiceDiscovery;

/**
 * Context of Scale Cube node. Used in {@link ServiceFactory}.
 *
 * @see ServiceFactory
 */
public interface MicroservicesContext {

  /**
   * Id of Scale Cube node.
   *
   * @return id
   */
  String id();

  /**
   * Used for remote service call.
   *
   * @return service call
   * @see ServiceCall
   */
  ServiceCall serviceCall();

  /**
   * Network address of Scale Cube node.
   *
   * @return address of node
   * @see Address
   */
  Address serviceAddress();

  /**
   * Service discovery for services localed in other nodes.
   *
   * @return service discovery
   * @see ServiceDiscovery
   */
  ServiceDiscovery serviceDiscovery();
}
