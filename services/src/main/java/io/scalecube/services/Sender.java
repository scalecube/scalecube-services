package io.scalecube.services;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

public interface Sender {

  void send(Address address, Message requestMessage, CompletableFuture<Void> messageFuture);

  Address address();

  Observable<Message> listen();

}
