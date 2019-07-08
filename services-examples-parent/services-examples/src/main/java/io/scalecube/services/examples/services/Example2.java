package io.scalecube.services.examples.services;

import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import reactor.core.publisher.Mono;
import reactor.core.scheduler.Schedulers;

public class Example2 {

  /**
   * Main method.
   *
   * @param args - program arguments
   */
  public static void main(String[] args) {
    Microservices gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(RSocketServiceTransport::new)
            .startAwait();

    Microservices service2Node =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opts ->
                                opts.membership(
                                    cfg -> cfg.seedMembers(gateway.discovery().address()))))
            .transport(RSocketServiceTransport::new)
            .services(new Service2Impl())
            .startAwait();

    Microservices service1Node =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opts ->
                                opts.membership(
                                    cfg -> cfg.seedMembers(gateway.discovery().address()))))
            .transport(RSocketServiceTransport::new)
            .services(new Service1Impl())
            .startAwait();

    gateway
        .call()
        .api(Service1.class)
        .remoteCallThenManyDelay(100)
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
