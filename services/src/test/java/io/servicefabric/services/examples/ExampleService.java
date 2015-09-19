package io.servicefabric.services.examples;

import io.servicefabric.services.annotations.Service;
import io.servicefabric.services.annotations.ServiceMethod;

/**
 * @author Anton Kharenko
 */
@Service("io.servicefabric.services.examples.ExampleService")
public interface ExampleService {

  @ServiceMethod("sayHello")
  void sayHello(HelloRequest request);

}
