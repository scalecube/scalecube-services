package io.scalecube.services.gateway.http;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.time.Duration;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

class HttpClientConnectionTest extends BaseTest {

  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices service;

  private static final AtomicInteger onCloseCounter = new AtomicInteger();

  @BeforeEach
  void beforEach() {
    gateway =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .gateway(options -> new HttpGateway.Builder().options(options.id("HTTP")).build())
            .startAwait();

    gatewayAddress = gateway.gateway("HTTP").address();

    service =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint))
                        .membership(
                            opts -> opts.seedMembers(gateway.discoveryAddress().toString())))
            .transport(RSocketServiceTransport::new)
            .services(new TestServiceImpl())
            .startAwait();

    onCloseCounter.set(0);
  }

  @AfterEach
  void afterEach() {
    Flux.concat(
            Mono.justOrEmpty(gateway).map(Microservices::shutdown),
            Mono.justOrEmpty(service).map(Microservices::shutdown))
        .then()
        .block();
  }

  @Test
  void testCloseServiceStreamAfterLostConnection() {
    try (ServiceCall serviceCall =
        new ServiceCall()
            .transport(new HttpGatewayClientTransport.Builder().address(gatewayAddress).build())
            .router(new StaticAddressRouter(gatewayAddress))) {
      StepVerifier.create(serviceCall.api(TestService.class).oneNever("body").log("<<< "))
          .thenAwait(Duration.ofSeconds(5))
          .then(serviceCall::close)
          .expectError(IOException.class)
          .verify(Duration.ofSeconds(1));
    }
  }

  @Test
  public void testCallRepeatedlyByInvalidAddress() {
    Address invalidAddress = Address.create("localhost", 5050);

    try (ServiceCall serviceCall =
        new ServiceCall()
            .transport(new HttpGatewayClientTransport.Builder().address(gatewayAddress).build())
            .router(new StaticAddressRouter(invalidAddress))) {
      for (int i = 0; i < 15; i++) {
        StepVerifier.create(serviceCall.api(TestService.class).oneNever("body").log("<<< "))
            .thenAwait(Duration.ofSeconds(1))
            .expectError(IOException.class)
            .verify(Duration.ofSeconds(10));
      }
    }
  }

  @Service
  public interface TestService {

    @ServiceMethod("oneNever")
    Mono<Long> oneNever(String name);
  }

  private static class TestServiceImpl implements TestService {

    @Override
    public Mono<Long> oneNever(String name) {
      return Mono.<Long>never().log(">>> ").doOnCancel(onCloseCounter::incrementAndGet);
    }
  }
}
