package io.scalecube.services;

import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import rx.Observable;

import java.util.Map;

public interface ServiceInstance {

  String serviceName();

  Object invoke(Message request) throws Exception;

  public <T> Observable<T> listen(Message request) throws Exception;
  
  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

 
}
