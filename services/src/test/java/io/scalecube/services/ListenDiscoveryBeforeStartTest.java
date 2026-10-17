package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.Test;

class ListenDiscoveryBeforeStartTest {

  @Test
  void listenDiscoveryBeforeStart() throws Exception {
    final var context =
        new Context()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint)))
            .transport(RSocketServiceTransport::new);

    final var events = new LinkedBlockingQueue<ServiceDiscoveryEvent>();
    final var subscription =
        context
            .listenDiscovery()
            .filter(ServiceDiscoveryEvent::isEndpointAdded)
            .subscribe(events::add);

    try (var gateway = Microservices.start(context);
        var provider = serviceProvider(gateway)) {
      final var event = events.poll(10, TimeUnit.SECONDS);
      assertNotNull(event, "no endpoint-added event");
      assertEquals(provider.id(), event.serviceEndpoint().id());
    } finally {
      subscription.dispose();
    }
  }

  private static Microservices serviceProvider(Microservices gateway) {
    return Microservices.start(
        new Context()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint))
                        .membership(cfg -> cfg.seedMembers(gateway.discoveryAddress().toString())))
            .transport(RSocketServiceTransport::new)
            .services(new GreetingServiceImpl()));
  }
}
