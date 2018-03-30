package io.scalecube.services;

import io.scalecube.transport.Address;

import java.util.Collection;
import java.util.Map;

public interface ServiceInstance {

  String serviceName();

  String memberId();

  Boolean isLocal();

  Map<String, String> tags();

  Address address();

  boolean methodExists(String methodName);

  void checkMethodExists(String header);

  Collection<String> methods();
}
