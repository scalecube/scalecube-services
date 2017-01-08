package io.scalecube.services;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.util.concurrent.CompletableFuture;

public interface ServiceCommunicator {

  CompletableFuture<Void> send(Address address, Message requestMessage);

  Address address();

  Observable<Message> listen();

}
