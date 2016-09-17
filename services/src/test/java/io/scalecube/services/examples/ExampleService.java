package io.scalecube.services.examples;

import com.google.common.util.concurrent.ListenableFuture;

import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.transport.Message;

/**
 * @author Anton Kharenko
 */
@Service
public interface ExampleService {

  @ServiceMethod
  ListenableFuture<Message> sayHello(Message request);

}
