package io.scalecube.services;

import static com.google.common.base.Preconditions.checkArgument;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

public class TransportSender implements Sender {

  private Transport transport;

  public TransportSender(Transport transport) {
    checkArgument(transport != null, "transport can't be null");
    this.transport = transport;
  }

  @Override
  public void send(Address address, Message message, CompletableFuture<Void> messageFuture) {
    this.transport.send(address, message, messageFuture);

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
