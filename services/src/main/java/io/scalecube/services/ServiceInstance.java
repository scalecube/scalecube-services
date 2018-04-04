package io.scalecube.services;

import io.scalecube.streams.StreamMessage;
import io.scalecube.transport.Address;

import rx.Observable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ServiceInstance {

  <TYPE> CompletableFuture<TYPE> invoke(StreamMessage request, Class<TYPE> responseType);

  Observable<StreamMessage> listen(StreamMessage request);

  String serviceName();

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

  boolean methodExists(String methodName);

  void checkMethodExists(String header);

  Collection<String> methods();



}
