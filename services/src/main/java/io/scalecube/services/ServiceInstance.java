package io.scalecube.services;

import io.scalecube.transport.Message;

public interface ServiceInstance {

  String serviceName();

  Object invoke(Message request, ServiceDefinition definition) throws Exception;

  String memberId();

  Boolean isLocal();

}
