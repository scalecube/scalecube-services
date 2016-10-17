package io.scalecube.services;

import io.scalecube.transport.Message;

public interface ServiceInstance {

  String qualifier();

  Object invoke(Message request) throws Exception;

  String memberId();

  Boolean isLocal();
}
