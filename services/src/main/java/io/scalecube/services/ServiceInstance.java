package io.scalecube.services;

import io.scalecube.transport.Message;

public interface ServiceInstance {

  String qualifier();

  <T> Object invoke(Message request, Class<T> returnType) throws Exception;

  String memberId();

  Boolean isLocal();
  
  boolean isReachable();
  
  String[] tags();
}
