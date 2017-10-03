package io.scalecube.services;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ServiceInstance {

  CompletableFuture<Message> invoke(Message request) ;

  Observable<Message> listen(Message request);
  
  String serviceName();

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

  boolean hasMethod(String methodName);

  void checkHasMethod(String header);
}
