package io.scalecube.services;

import io.scalecube.transport.Message;

public interface ServiceInstance {

  String qualifier();

  Object invoke(Message request, Class<?> returnType) throws Exception;

  String memberId();

  Boolean isLocal();
  
  boolean isReachable();
  
  String[] tags();
}
