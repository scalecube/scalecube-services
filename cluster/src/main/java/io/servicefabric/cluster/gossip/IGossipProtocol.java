package io.servicefabric.cluster.gossip;

import io.servicefabric.transport.Message;

import rx.Observable;

/**
 * Gossip Protocol component responsible for spreading information (gossips) over the cluster members using
 * infection-style dissemination algorithms. It provides reliable cross-cluster broadcast.
 *
 * @author Anton Kharenko
 */
public interface IGossipProtocol {

  /** Spreads given message between cluster members. */
  void spread(Message message);

  /** Listens for gossips from other cluster members. */
  Observable<Message> listen();

}
