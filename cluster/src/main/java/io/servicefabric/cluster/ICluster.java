package io.servicefabric.cluster;

import io.servicefabric.cluster.gossip.IGossipProtocol;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.servicefabric.transport.Message;
import rx.Observable;

/**
 * @author Anton Kharenko
 */
public interface ICluster {

  void send(ClusterMember member, Message message);

  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  Observable<Message> listen();

  IGossipProtocol gossip();

  IClusterMembership membership();

  ICluster join();

  ListenableFuture<Void> leave();

}
