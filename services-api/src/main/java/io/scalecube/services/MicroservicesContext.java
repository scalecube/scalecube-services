package io.scalecube.services;

import io.scalecube.net.Address;

/**
 * Context of Scalecube node. Used in {@link ServiceFactory}.
 *
 * @see ServiceFactory
 */
public interface MicroservicesContext {

  /**
   * Id of Scalecube node.
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
   * Network address of Scalecube node.
   *
   * @return address of node
   * @see Address
   */
  Address serviceAddress();
}
