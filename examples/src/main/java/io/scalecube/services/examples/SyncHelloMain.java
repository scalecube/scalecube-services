package io.scalecube.services.examples;

import io.scalecube.services.Microservices;
import io.scalecube.services.examples.GreetingServiceImpl;

/**
 * Example of scalecube services invoking a service sync
 * simpleSyncInvoke - invoke the service within single cluster node.
 * distributedAsyncInvoke -invoke a service utilizing 2 cluster nodes
 * 
 * <p>the requests are Sync (blocking) meaning requester waits for a result.
 * @param args N/A
 * @throws Exception runtime exception.
 */
public class SyncHelloMain {

  public static void main(String[] args) {

    simpleBlockingCallExample();

    distributedBlockingCallExample();
  }

  private static void simpleBlockingCallExample() {
    // Create microservices instance.
    GreetingService service = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build()
        .proxy().api(GreetingService.class)
        .create();

    // call the service.
    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);
  }

  private static void distributedBlockingCallExample() {
    // Create microservices cluster.
    Microservices provider = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = Microservices.builder()
        .seeds(provider.cluster().address()) // join provider cluster
        .build().proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();

    String result = service.greeting("joe");

    // print the greeting.
    System.out.println(result);

    System.exit(0);
  }


}
