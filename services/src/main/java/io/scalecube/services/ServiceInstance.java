package io.scalecube.services;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ServiceInstance {

  String serviceName();

  CompletableFuture<Message> invoke(Message request) ;

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();
}
