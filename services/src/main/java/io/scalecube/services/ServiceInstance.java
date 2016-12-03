package io.scalecube.services;

import io.scalecube.transport.Message;

import java.util.Map;

public interface ServiceInstance {

  String serviceName();

  Object invoke(Message request, ServiceDefinition definition) throws Exception;

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();
}
