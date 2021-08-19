package io.scalecube.services.examples.services;

import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
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
            .discovery(
                "gateway",
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory())))
            .transport(RSocketServiceTransport::new)
            .startAwait();

    final Address gatewayAddress = gateway.discovery("gateway").address();

    Microservices service2Node =
        Microservices.builder()
            .discovery(
                "service2Node",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .membership(cfg -> cfg.seedMembers(gatewayAddress)))
            .transport(RSocketServiceTransport::new)
            .services(new Service2Impl())
            .startAwait();

    Microservices service1Node =
        Microservices.builder()
            .discovery(
                "service1Node",
                endpoint ->
                    new ScalecubeServiceDiscovery(endpoint)
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .membership(cfg -> cfg.seedMembers(gatewayAddress)))
            .transport(RSocketServiceTransport::new)
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
