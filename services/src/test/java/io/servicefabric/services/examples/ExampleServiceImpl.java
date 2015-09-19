package io.servicefabric.services.examples;

import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * @author Anton Kharenko
 */
public class ExampleServiceImpl implements ExampleService {

  @Override
  public ListenableFuture<HelloResponse> sayHello(HelloRequest request) {
	System.out.println("Hello" + request );
    return Futures.immediateFuture(new HelloResponse());
  }
}
