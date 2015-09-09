package io.servicefabric.cluster;

/**
 * Extends cluster membership protocol interface and provides management operations. This interface is supposed for internal use.
 *
 * @author Anton Kharenko
 */
public interface IManagedClusterMembership extends IClusterMembership {

  /**
   * Starts running cluster membership protocol. After started it begins to receive and send cluster membership messages
   */
  void start();

  /** Stops running cluster membership protocol and releases occupied resources. */
  void stop();

  /**
   * Denoting fact that local member is getting gracefully shutdown. It will notify other members that going to be stopped soon. After
   * calling this method recommended to wait some reasonable amount of time to start spreading inforamtion about leave before stopping
   * server.
   */
  void leave();

}
