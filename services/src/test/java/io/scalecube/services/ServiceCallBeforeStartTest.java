package io.scalecube.services;

import io.scalecube.services.Microservices.Context;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.exceptions.ServiceUnavailableException;
import io.scalecube.services.sut.GreetingService;
import io.scalecube.services.sut.GreetingServiceImpl;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import org.junit.jupiter.api.Test;
import reactor.test.StepVerifier;
import reactor.util.retry.Retry;

class ServiceCallBeforeStartTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  @Test
  void localProxyCreatedBeforeStart() {
    final var context = new Context().services(new GreetingServiceImpl());
    final var greetingService = context.call().api(GreetingService.class);

    StepVerifier.create(greetingService.greetingNoParams())
        .expectError(ServiceUnavailableException.class)
        .verify(TIMEOUT);

    try (var microservices = Microservices.start(context)) {
      StepVerifier.create(greetingService.greetingNoParams())
          .expectNextCount(1)
          .expectComplete()
          .verify(TIMEOUT);
    }
  }

  @Test
  void remoteProxyCreatedBeforeStart() {
    final var context =
        new Context()
            .discovery(
                endpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(endpoint)))
            .transport(RSocketServiceTransport::new);
    final var greetingService = context.call().api(GreetingService.class);

    StepVerifier.create(greetingService.greetingNoParams())
        .expectError(ServiceUnavailableException.class)
        .verify(TIMEOUT);

    final var gateway = Microservices.start(context);
    try (var provider = serviceProvider(gateway)) {
      StepVerifier.create(
              greetingService
                  .greetingNoParams()
                  .retryWhen(Retry.fixedDelay(Long.MAX_VALUE, Duration.ofMillis(100))))
          .expectNextCount(1)
          .expectComplete()
          .verify(TIMEOUT);

      gateway.close();

      StepVerifier.create(greetingService.greetingNoParams())
          .expectError(ServiceUnavailableException.class)
          .verify(TIMEOUT);
    } finally {
      gateway.close();
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
