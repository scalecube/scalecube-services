package io.scalecube.cluster.gossip;

import io.scalecube.transport.Address;
import io.scalecube.transport.IListenable;
import io.scalecube.transport.Message;

import rx.Observable;
import rx.Scheduler;

import java.util.Collection;
import java.util.concurrent.Executor;

/**
 * Gossip Protocol component responsible for spreading information (gossips) over the cluster members using
 * infection-style dissemination algorithms. It provides reliable cross-cluster broadcast.
 *
 * @author Anton Kharenko
 */
public interface IGossipProtocol extends IListenable<Message> {

  /**
   * Starts running gossip protocol. After started it begins to receive and send gossip messages.
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
   * Updates list of cluster members among which should be spread gossips.
   */
  void setMembers(Collection<Address> members);

  /**
   * Listens for gossips from other cluster members.
   */
  @Override
  Observable<Message> listen();

  /**
   * Listens for gossips from other cluster members.
   */
  @Override
  Observable<Message> listen(Executor executor);

  /**
   * Listens for gossips from other cluster members.
   */
  @Override
  Observable<Message> listen(Scheduler scheduler);
}
