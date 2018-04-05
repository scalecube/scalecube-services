package io.scalecube.services;

import io.scalecube.streams.StreamMessage;
import io.scalecube.transport.Address;

import rx.Observable;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

public interface ServiceInstance {

  CompletableFuture<StreamMessage> invoke(StreamMessage request);

  Observable<StreamMessage> listen(StreamMessage request);

  <TYPE> CompletableFuture<StreamMessage> invoke(StreamMessage request, Class<TYPE> responseType);

  <RESP_TYPE> Observable<RESP_TYPE> listen(StreamMessage request, Class<RESP_TYPE> responseType);

  String serviceName();

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

  boolean methodExists(String methodName);

  void checkMethodExists(String header);

  Collection<String> methods();



}
