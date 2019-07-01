package io.scalecube.services.examples.services;

import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class Example1 {

  /**
   * Main method.
   *
   * @param args - program arguments
   */
  public static void main(String[] args) {
    Microservices gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .startAwait();

    Microservices service2Node =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.seedMembers(gateway.discovery().address())))
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .services(new Service2Impl())
            .startAwait();

    Microservices service1Node =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(opts -> opts.seedMembers(gateway.discovery().address())))
            .transport(opts -> opts.serviceTransport(RSocketServiceTransport::new))
            .services(new Service1Impl())
            .startAwait();

    gateway
        .call()
        .api(Service1.class)
        .manyDelay(100)
        .publishOn(Schedulers.parallel())
        .take(10)
        .log("receive     |")
        .collectList()
        .log("complete    |")
        .block();

    Mono.whenDelayError(gateway.shutdown(), service1Node.shutdown(), service2Node.shutdown())
        .block();
  }
}
