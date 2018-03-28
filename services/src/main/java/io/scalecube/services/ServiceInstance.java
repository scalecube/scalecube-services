package io.scalecube.services;

import io.scalecube.streams.StreamMessage;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.time.Duration;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ServiceInstance {

  CompletableFuture<Message> invoke(Message request, Duration duration) ;
  CompletableFuture<StreamMessage> invoke(StreamMessage request, Duration duration);

  Observable<Message> listen(Message request, Duration duration);
  Observable<StreamMessage> listen(StreamMessage request, Duration duration);
  
  String serviceName();

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

  boolean methodExists(String methodName);

  void checkMethodExists(String header);

  Collection<String> methods();



}
