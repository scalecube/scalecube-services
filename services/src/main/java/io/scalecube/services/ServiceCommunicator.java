package io.scalecube.services;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import io.reactivex.Flowable;

import java.util.concurrent.CompletableFuture;

public interface ServiceCommunicator {

  CompletableFuture<Void> send(Address address, Message requestMessage);

  Address address();

  Flowable<Message> listen();

}
