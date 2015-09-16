package io.servicefabric.services.examples;

import io.servicefabric.services.ServiceRegistry;

/**
 * @author Anton Kharenko
 */
public class Main {

  public static void main(String[] args) {
    ServiceRegistry sr = new ServiceRegistry(null);
    ExampleServiceImpl exampleService = new ExampleServiceImpl();
    sr.registerService(exampleService);

  }

}
