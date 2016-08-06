package io.scalecube.cluster.gossip;

import io.scalecube.transport.Address;

import java.util.Collection;

/**
 * Extends gossip protocol interface and provides management operations. This interface is supposed for internal use.
 *
 * @author Anton Kharenko
 */
public interface IManagedGossipProtocol extends IGossipProtocol {

  /** Updates list of cluster members among which should be spread gossips. */
  void setClusterMembers(Collection<Address> members);

  /** Starts running gossip protocol. After started it begins to receive and send gossip messages */
  void start();

  /** Stops running gossip protocol and releases occupied resources. */
  void stop();

}
