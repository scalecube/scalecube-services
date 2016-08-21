package io.scalecube.cluster;

import io.scalecube.transport.IListenable;
import io.scalecube.transport.Message;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;
import rx.Scheduler;

import java.util.concurrent.Executor;

/**
 * Basic cluster interface which allows to join cluster, send message to other member, listen messages, gossip messages.
 * 
 * @author Anton Kharenko
 */
public interface ICluster extends IListenable<Message> {

  IClusterMembership membership();

  ListenableFuture<Void> leave();

  void send(ClusterMember member, Message message);

  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  /**
   * Spreads given message between cluster members.
   */
  void spreadGossip(Message message);

  /**
   * Listens for gossips from other cluster members.
   */
  Observable<Message> listenGossips();

  /**
   * Listens for gossips from other cluster members.
   */
  Observable<Message> listenGossips(Executor executor);

  /**
   * Listens for gossips from other cluster members.
   */
  Observable<Message> listenGossips(Scheduler scheduler);
}
