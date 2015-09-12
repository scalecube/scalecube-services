package io.servicefabric.cluster;

import io.servicefabric.cluster.gossip.IGossipProtocol;
import io.servicefabric.transport.TransportMessage;

import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;

import io.servicefabric.transport.protocol.Message;
import rx.Observable;

/**
 * @author Anton Kharenko
 */
public interface ICluster {

  void send(ClusterMember member, Message message);

  void send(ClusterMember member, Message message, SettableFuture<Void> promise);

  Observable<ClusterMessage> listen();

  IGossipProtocol gossip();

  IClusterMembership membership();

  ICluster join();

  ListenableFuture<Void> leave();

}
