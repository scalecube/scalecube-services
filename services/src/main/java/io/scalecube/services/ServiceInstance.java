package io.scalecube.services;

import java.lang.reflect.Type;
import java.util.Optional;

import io.scalecube.transport.Message;

public interface ServiceInstance {

  String qualifier();

  <T> Object invoke(Message request, Optional<ServiceDefinition> definition) throws Exception;

  String memberId();

  Boolean isLocal();

  boolean isReachable();

  String[] tags();
  
  Type returnType();

}
