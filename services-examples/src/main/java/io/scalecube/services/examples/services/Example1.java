package io.scalecube.services.examples.services;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import reactor.core.scheduler.Schedulers;

public class Example1 {

  /**
   * Main method.
   *
   * @param args - program arguments
   */
  public static void main(String[] args) {
    Microservices gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new));

    final Address gatewayAddress = gateway.discoveryAddress();

    Microservices service2Node =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(new Service2Impl()));

    Microservices service1Node =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(new Service1Impl()));

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

    gateway.close();
    service1Node.close();
    service2Node.close();
  }
}
