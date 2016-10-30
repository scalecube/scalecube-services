package io.scalecube.services;

import io.scalecube.transport.Message;

import java.util.Optional;

public interface ServiceInstance {

  String qualifier();

  <T> Object invoke(Message request, Optional<ServiceDefinition> definition) throws Exception;

  String memberId();

  Boolean isLocal();

  boolean isReachable();

  String[] tags();
}
