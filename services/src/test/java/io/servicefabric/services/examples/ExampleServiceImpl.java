package io.servicefabric.services.examples;


/**
 * @author Anton Kharenko
 */
public class ExampleServiceImpl implements ExampleService {

  @Override
  public void sayHello(HelloRequest request) {
	System.out.println("Hello: " + request );
  }
  
}
