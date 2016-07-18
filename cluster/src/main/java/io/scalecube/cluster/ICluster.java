package io.scalecube.cluster;

import io.scalecube.cluster.gossip.IGossipProtocol;
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

  void send(ClusterMember member, Message message);

  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  Observable<Message> listen();

  IGossipProtocol gossip();

  IClusterMembership membership();

  ListenableFuture<Void> leave();

}
