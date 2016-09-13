package io.scalecube.services;

import java.util.Set;

import io.scalecube.transport.Message;

/**
 * @author Anton Kharenko
 */
public interface ServiceInstance {

  String serviceName();

  Set<String> methodNames();

  // TODO [AK]: Exception... clean up.
  Object invoke(String methodName, Message request) throws Exception;

}
