package io.scalecube.services;

import static io.scalecube.utils.Preconditions.checkArgument;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

public class ServiceTransport implements ServiceCommunicator {

  private Transport transport;
  private Cluster cluster;

  public ServiceTransport(Transport transport) {
    checkArgument(transport != null, "transport can't be null");
    this.transport = transport;
  }

  @Override
  public CompletableFuture<Void> send(Address address, Message message) {
    CompletableFuture<Void> future = new CompletableFuture<Void>();
    transport.send(address, message, future);
    return future;
  }

  @Override
  public Address address() {
    return this.transport.address();
  }

  @Override
  public Observable<Message> listen() {
    return this.transport.listen();
  }

  public Cluster cluster() {
    return this.cluster;
  }

  public void cluster(Cluster cluster) {
    this.cluster = cluster;
  }

  @Override
  public CompletableFuture<Void> shutdown() {
    CompletableFuture<Void> promise = new CompletableFuture<Void>();
    this.transport.stop(promise);
    return promise;
  }

  @Override
  public boolean isStopped() {
    return cluster.isShutdown();
  }


}
