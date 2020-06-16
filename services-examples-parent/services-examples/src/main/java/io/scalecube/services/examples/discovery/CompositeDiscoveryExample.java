package io.scalecube.services.examples.discovery;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ScalecubeServiceFactory;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.examples.helloworld.service.api.Greeting;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import reactor.core.publisher.Mono;

public class CompositeDiscoveryExample {

  /**
   * Main program.
   *
   * @param args arguments
   */
  public static void main(String[] args) throws InterruptedException {
    Microservices seed1 =
        Microservices.builder()
            .discovery("seed1", ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Microservices seed2 =
        Microservices.builder()
            .discovery("seed2", ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    final Address seed1Address = seed1.context().discovery("seed1").address();
    final Address seed2Address = seed2.context().discovery("seed2").address();

    Microservices ms1 =
        Microservices.builder()
            .discovery(
                "ms1",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seed1Address)))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(ScalecubeServiceFactory.fromInstances(new GreetingServiceImpl1()))
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(
                "ms2",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .membership(cfg -> cfg.seedMembers(seed2Address)))
            .transport(RSocketServiceTransport::new)
            .serviceFactory(ScalecubeServiceFactory.fromInstances(new GreetingServiceImpl2()))
            .startAwait();

    Microservices compositeMs =
        Microservices.builder()
            .discovery(
                "domain1",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .options(cfg -> cfg.memberIdGenerator(endpoint::id))
                        .membership(cfg -> cfg.seedMembers(seed1Address)))
            .discovery(
                "domain2",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .options(cfg -> cfg.memberIdGenerator(endpoint::id))
                        .membership(cfg -> cfg.seedMembers(seed2Address)))
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Greeting greeting1 =
        compositeMs
            .context()
            .serviceCall()
            .api(GreetingsService1.class)
            .sayHello("hello one")
            .block();
    System.err.println("This is response from GreetingsService1: " + greeting1.message());

    Greeting greeting2 =
        compositeMs
            .context()
            .serviceCall()
            .api(GreetingsService2.class)
            .sayHello("hello two")
            .block();
    System.err.println("This is response from GreetingsService2: " + greeting2.message());

    Thread.currentThread().join();
  }

  @Service
  public interface GreetingsService1 {

    @ServiceMethod
    Mono<Greeting> sayHello(String name);
  }

  @Service
  public interface GreetingsService2 {

    @ServiceMethod
    Mono<Greeting> sayHello(String name);
  }

  public static class GreetingServiceImpl1 implements GreetingsService1 {

    @Override
    public Mono<Greeting> sayHello(String name) {
      return Mono.just(
          new Greeting(
              "This is GreetingServiceImpl1: nice to meet you \""
                  + name
                  + "\" and welcome to ScaleCube"));
    }
  }

  public static class GreetingServiceImpl2 implements GreetingsService2 {

    @Override
    public Mono<Greeting> sayHello(String name) {
      return Mono.just(
          new Greeting(
              "This is GreetingServiceImpl2: nice to meet you \""
                  + name
                  + "\" and welcome to ScaleCube"));
    }
  }
}
