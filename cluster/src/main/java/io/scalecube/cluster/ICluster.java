package io.scalecube.cluster;

import io.scalecube.cluster.gossip.IGossipProtocol;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;

/**
 * Basic cluster interface which allows to join cluster, send message to other member, listen messages, gossip messages.
 * 
 * @author Anton Kharenko
 */
public interface ICluster {


  void send(Address address, Message message);
  void send(ClusterMember member, Message message);

  void send(Address address, Message message, SettableFuture<Void> promise);
  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  Observable<Message> listen();

  /** Spreads given message between cluster members. */
  void spreadGossip(Message message);

  /** Listens for gossips from other cluster members. */
  Observable<Message> listenGossips();

  IClusterMembership membership();

  ListenableFuture<Void> leave();

}
