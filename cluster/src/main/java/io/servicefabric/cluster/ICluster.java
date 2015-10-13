package io.servicefabric.cluster;

import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.Message;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import rx.Observable;

/**
 * Basic cluster interface which allows to join cluster, send message to other member, listen messages, gossip messages.
 * 
 * @author Anton Kharenko
 */
public interface ICluster {

  void send(ClusterMember member, Message message);

  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  Observable<Message> listen();

  IGossipProtocol gossip();

  IClusterMembership membership();

  ListenableFuture<ICluster> join();

  ICluster joinAwait();

  ListenableFuture<Void> leave();

}
