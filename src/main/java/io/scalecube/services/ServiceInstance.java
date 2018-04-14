package io.scalecube.services;

import io.scalecube.services.transport.api.ServiceMessage;
import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.Map;
import java.util.concurrent.CompletableFuture;

import reactor.core.publisher.Flux;

public interface ServiceInstance {

  CompletableFuture<ServiceMessage> invoke(ServiceMessage request);

  <T> Flux<T> listen(ServiceMessage request);
  
  String serviceName();

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

  boolean methodExists(String methodName);

  void checkMethodExists(String header);

  Collection<String> methods();

 

}
