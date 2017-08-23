package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import io.reactivex.Flowable;

import java.util.concurrent.CompletableFuture;

public class ClusterServiceCommunicator implements ServiceCommunicator {

  private Cluster cluster;

  public ClusterServiceCommunicator(Cluster cluster) {
    checkArgument(cluster != null, "cluster can't be null");
    this.cluster = cluster;
  }

  @Override
  public CompletableFuture<Void> send(Address address, Message message) {
    CompletableFuture<Void> messageFuture = new CompletableFuture<>();
    cluster.send(address, message, messageFuture);
    return messageFuture;
  }

  @Override
  public Address address() {
    return cluster.address();
  }

  @Override
  public Flowable<Message> listen() {
    return cluster.listen();
  }

}
