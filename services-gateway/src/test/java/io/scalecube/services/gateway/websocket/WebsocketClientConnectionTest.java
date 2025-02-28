package io.scalecube.services.gateway.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertInstanceOf;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class WebsocketClientConnectionTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(10);

  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices microservices;
  private final TestGatewaySessionHandler sessionEventHandler = new TestGatewaySessionHandler();

  private static final AtomicInteger onCloseCounter = new AtomicInteger();

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
                .gateway(
                    () ->
                        new WebsocketGateway.Builder()
                            .id("WS")
                            .gatewayHandler(sessionEventHandler)
                            .build()));

    gatewayAddress = gateway.gateway("WS").address();

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
                .services(new TestServiceImpl(onCloseCounter::incrementAndGet)));

    onCloseCounter.set(0);
  }

  @AfterEach
  void afterEach() {
    gateway.close();
    microservices.close();
  }

  @Test
  void testCloseServiceStreamAfterLostConnection() {
    try (ServiceCall serviceCall = serviceCall(gatewayAddress)) {
      StepVerifier.create(serviceCall.api(TestService.class).manyNever().log("<<<"))
          .thenAwait(Duration.ofSeconds(5))
          .then(serviceCall::close)
          .expectError(IOException.class)
          .verify(TIMEOUT);

      Mono.delay(Duration.ofMillis(100))
          .repeat(() -> onCloseCounter.get() != 1)
          .then()
          .block(TIMEOUT);

      assertEquals(1, onCloseCounter.get());
    }
  }

  @Test
  public void testCallRepeatedlyByInvalidAddress() {
    try (ServiceCall serviceCall = serviceCall(Address.create("localhost", 5050))) {
      for (int i = 0; i < 15; i++) {
        StepVerifier.create(serviceCall.api(TestService.class).manyNever().log("<<<"))
            .thenAwait(Duration.ofSeconds(1))
            .expectErrorSatisfies(
                ex -> {
                  final Throwable cause = ex.getCause();
                  assertNotNull(cause, "cause");
                  assertInstanceOf(IOException.class, cause);
                })
            .verify(TIMEOUT);
      }
    }
  }

  @Test
  public void testHandlerEvents() throws Exception {
    try (final ServiceCall serviceCall = serviceCall(gatewayAddress)) {
      serviceCall.api(TestService.class).one("one").block(TIMEOUT);
      assertTrue(sessionEventHandler.connLatch.await(3, TimeUnit.SECONDS));
      assertEquals(0, sessionEventHandler.connLatch.getCount());

      assertTrue(sessionEventHandler.msgLatch.await(3, TimeUnit.SECONDS));
      assertEquals(0, sessionEventHandler.msgLatch.getCount());

      serviceCall.close();
      assertTrue(sessionEventHandler.disconnLatch.await(3, TimeUnit.SECONDS));
      assertEquals(0, sessionEventHandler.disconnLatch.getCount());
    }
  }

  @Test
  void testClientHeaders() {
    final String headerKey = "secret-token";
    final String headerValue = UUID.randomUUID().toString();

    try (final ServiceCall serviceCall =
        new ServiceCall()
            .transport(
                new WebsocketGatewayClientTransport.Builder()
                    .address(gatewayAddress)
                    .headers(Collections.singletonMap(headerKey, headerValue))
                    .build())
            .router(StaticAddressRouter.from(gatewayAddress).build())) {
      StepVerifier.create(
              serviceCall
                  .api(TestService.class)
                  .one("one")
                  .then(Mono.fromCallable(sessionEventHandler::lastSession)))
          .assertNext(session -> assertEquals(headerValue, session.headers().get(headerKey)))
          .expectComplete()
          .verify(TIMEOUT);
    }
  }

  private static ServiceCall serviceCall(Address address) {
    return new ServiceCall()
        .transport(new WebsocketGatewayClientTransport.Builder().address(address).build())
        .router(StaticAddressRouter.from(address).build());
  }
}
