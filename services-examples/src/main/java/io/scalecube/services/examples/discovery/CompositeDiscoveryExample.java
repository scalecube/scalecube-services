package io.scalecube.services.examples.discovery;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.annotations.AfterConstruct;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryContext;
import io.scalecube.services.examples.helloworld.service.api.Greeting;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import reactor.core.publisher.Mono;

public class CompositeDiscoveryExample {

  /**
   * Main program.
   *
   * @param args arguments
   */
  public static void main(String[] args) {
    Microservices seed1 =
        Microservices.builder()
            .discovery(
                "seed1",
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint))
                        .options(opts -> opts.memberAlias("seed1")))
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Microservices seed2 =
        Microservices.builder()
            .discovery(
                "seed2",
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint))
                        .options(opts -> opts.memberAlias("seed2")))
            .transport(RSocketServiceTransport::new)
            .startAwait();

    final Address seed1Address = seed1.discovery("seed1").address();
    final Address seed2Address = seed2.discovery("seed2").address();

    Microservices ms1 =
        Microservices.builder()
            .discovery(
                "ms1",
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .options(opts -> opts.memberAlias("ms1"))
                        .membership(cfg -> cfg.seedMembers(seed1Address)))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl1())
            .startAwait();

    Microservices ms2 =
        Microservices.builder()
            .discovery(
                "ms2",
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .options(opts -> opts.memberAlias("ms2"))
                        .membership(cfg -> cfg.seedMembers(seed2Address)))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl2())
            .startAwait();

    Microservices compositeMs =
        Microservices.builder()
            .discovery(
                "domain1",
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .options(opts -> opts.memberAlias("domain1"))
                        .membership(cfg -> cfg.seedMembers(seed1Address)))
            .discovery(
                "domain2",
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .options(opts -> opts.memberAlias("domain2"))
                        .membership(cfg -> cfg.seedMembers(seed2Address)))
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Greeting greeting1 =
        compositeMs.call().api(GreetingsService1.class).sayHello("hello one").block();
    System.err.println("This is response from GreetingsService1: " + greeting1.message());

    Greeting greeting2 =
        compositeMs.call().api(GreetingsService2.class).sayHello("hello two").block();
    System.err.println("This is response from GreetingsService2: " + greeting2.message());
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

    @AfterConstruct
    void init(Microservices ms) {
      ServiceDiscoveryContext discoveryContext = ms.discovery("ms1");
      System.err.println("discovery(\"ms1\"): " + discoveryContext);
      discoveryContext
          .listen()
          .subscribe(
              discoveryEvent -> System.err.println("discovery(\"ms1\") event: " + discoveryEvent));
    }

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

    @AfterConstruct
    void init(Microservices ms) {
      ServiceDiscoveryContext discoveryContext = ms.discovery("ms2");
      System.err.println("discovery(\"ms2\"): " + discoveryContext);
      discoveryContext
          .listen()
          .subscribe(
              discoveryEvent -> System.err.println("discovery(\"ms2\") event: " + discoveryEvent));
    }

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
