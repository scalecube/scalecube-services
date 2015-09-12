package io.servicefabric.services.examples;

import com.google.common.util.concurrent.ListenableFuture;

import io.servicefabric.services.annotations.Service;
import io.servicefabric.services.annotations.ServiceMethod;

/**
 * @author Anton Kharenko
 */
@Service("io.servicefabric.services.examples.ExampleService")
public interface ExampleService {

  @ServiceMethod("sayHello")
  ListenableFuture<HelloResponse> sayHello(HelloRequest request);

}
