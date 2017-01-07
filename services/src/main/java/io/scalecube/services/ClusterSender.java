package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

public class ClusterSender implements Sender {

  private Cluster cluster;

  public ClusterSender(Cluster cluster) {
    checkArgument(cluster != null, "cluster can't be null");
    this.cluster = cluster;
  }

  @Override
  public void send(Address address, Message message, CompletableFuture<Void> messageFuture) {
    cluster.send(address, message, messageFuture);
  }

  @Override
  public Address address() {
    return cluster.address();
  }

  @Override
  public Observable<Message> listen() {
    return cluster.listen();
  }

}
