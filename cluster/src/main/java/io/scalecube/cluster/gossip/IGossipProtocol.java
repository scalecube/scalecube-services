package io.scalecube.cluster.gossip;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.util.Collection;

/**
 * Gossip Protocol component responsible for spreading information (gossips) over the cluster members using
 * infection-style dissemination algorithms. It provides reliable cross-cluster broadcast.
 *
 * @author Anton Kharenko
 */
public interface IGossipProtocol {

  /**
   * Starts running gossip protocol. After started it begins to receive and send gossip messages
   */
  void start();

  /**
   * Stops running gossip protocol and releases occupied resources.
   */
  void stop();

  /**
   * Spreads given message between cluster members.
   */
  void spread(Message message);

  /**
   * Listens for gossips from other cluster members.
   */
  Observable<Message> listen();

  /**
   * Updates list of cluster members among which should be spread gossips.
   */
  void setMembers(Collection<Address> members);

}
