package io.scalecube.services;

import io.scalecube.transport.Message;

public interface ServiceInstance {

  String serviceName();

  Object invoke(String methodName, Message request) throws Exception;

  String memberId();

  Boolean isLocal();
}
