package io.scalecube.services.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class HttpClientConnectionTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices microservices;

  private final AtomicInteger onCancelCounter = new AtomicInteger();

  @BeforeEach
  void beforEach() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint)))
                .transport(RSocketServiceTransport::new)
                .gateway(() -> HttpGateway.builder().id("HTTP").build()));

    gatewayAddress = gateway.gateway("HTTP").address();

    microservices =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(
                                opts -> opts.seedMembers(gateway.discoveryAddress().toString())))
                .transport(RSocketServiceTransport::new)
                .services(new TestServiceImpl(onCancelCounter)));
  }

  @AfterEach
  void afterEach() {
    gateway.close();
    microservices.close();
  }

  @Test
  void testCloseServiceStreamAfterLostConnection() {
    try (ServiceCall serviceCall = serviceCall(gatewayAddress)) {
      StepVerifier.create(serviceCall.api(TestService.class).oneNever("body").log("<<<"))
          .thenAwait(Duration.ofSeconds(3))
          .then(serviceCall::close)
          .expectError(IOException.class)
          .verify(TIMEOUT);

      Mono.delay(Duration.ofMillis(100))
          .repeat(() -> onCancelCounter.get() != 1)
          .then()
          .block(TIMEOUT);

      assertEquals(1, onCancelCounter.get());
    }
  }

  @Test
  public void testCallRepeatedlyByInvalidAddress() {
    final Address address = Address.create("localhost", 5050);
    try (ServiceCall serviceCall = serviceCall(address)) {
      for (int i = 0; i < 15; i++) {
        StepVerifier.create(serviceCall.api(TestService.class).oneNever("body").log("<<<"))
            .thenAwait(Duration.ofSeconds(1))
            .expectError(IOException.class)
            .verify(TIMEOUT);
      }
    }
  }

  private static ServiceCall serviceCall(Address address) {
    return new ServiceCall()
        .logger("serviceCall")
        .transport(HttpGatewayClientTransport.builder().address(address).build())
        .router(StaticAddressRouter.forService(address, "app-service").build());
  }

  @Service
  public interface TestService {

    @ServiceMethod
    Mono<Void> oneNever(String name);
  }

  private static class TestServiceImpl implements TestService {

    private final AtomicInteger onCancelCounter;

    public TestServiceImpl(AtomicInteger onCancelCounter) {
      this.onCancelCounter = onCancelCounter;
    }

    @Override
    public Mono<Void> oneNever(String name) {
      return Mono.never().log(">>>").doOnCancel(onCancelCounter::incrementAndGet).then();
    }
  }
}
