package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

public class TransportServiceCommunicator implements ServiceCommunicator {

  private Transport transport;

  public TransportServiceCommunicator(Transport transport) {
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
}
