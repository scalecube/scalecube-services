package io.scalecube.cluster;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;

/**
 * Facade cluster interface which provides API to interact with cluster members.
 * 
 * @author Anton Kharenko
 */
public interface ICluster {

  /**
   * Returns local listen {@link Address} of this cluster instance.
   */
  Address localAddress();

  void send(ClusterMember member, Message message);

  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  Observable<Message> listen();

  /** Spreads given message between cluster members. */
  void spreadGossip(Message message);

  /** Listens for gossips from other cluster members. */
  Observable<Message> listenGossips();

  IClusterMembership membership();

  ListenableFuture<Void> leave();

}
