package io.scalecube.services.gateway.http;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.transport.GatewayClient;
import io.scalecube.services.gateway.transport.GatewayClientCodec;
import io.scalecube.services.gateway.transport.GatewayClientSettings;
import io.scalecube.services.gateway.transport.GatewayClientTransport;
import io.scalecube.services.gateway.transport.GatewayClientTransports;
import io.scalecube.services.gateway.transport.StaticAddressRouter;
import io.scalecube.services.gateway.transport.http.HttpGatewayClient;
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

  public static final GatewayClientCodec<ByteBuf> CLIENT_CODEC =
      GatewayClientTransports.HTTP_CLIENT_CODEC;

  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices service;

  private static final AtomicInteger onCloseCounter = new AtomicInteger();
  private GatewayClient client;

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
            .gateway(options -> new HttpGateway(options.id("HTTP")))
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
            Mono.justOrEmpty(client).doOnNext(GatewayClient::close).flatMap(GatewayClient::onClose),
            Mono.justOrEmpty(gateway).map(Microservices::shutdown),
            Mono.justOrEmpty(service).map(Microservices::shutdown))
        .then()
        .block();
  }

  @Test
  void testCloseServiceStreamAfterLostConnection() {
    client =
        new HttpGatewayClient(
            GatewayClientSettings.builder().address(gatewayAddress).build(), CLIENT_CODEC);

    ServiceCall serviceCall =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(gatewayAddress));

    StepVerifier.create(serviceCall.api(TestService.class).oneNever("body").log("<<< "))
        .thenAwait(Duration.ofSeconds(5))
        .then(() -> client.close())
        .then(() -> client.onClose().block())
        .expectError(IOException.class)
        .verify(Duration.ofSeconds(1));
  }

  @Test
  public void testCallRepeatedlyByInvalidAddress() {
    Address invalidAddress = Address.create("localhost", 5050);

    client =
        new HttpGatewayClient(
            GatewayClientSettings.builder().address(invalidAddress).build(), CLIENT_CODEC);

    ServiceCall serviceCall =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(invalidAddress));

    for (int i = 0; i < 100; i++) {
      StepVerifier.create(serviceCall.api(TestService.class).oneNever("body").log("<<< "))
          .thenAwait(Duration.ofSeconds(1))
          .expectError(IOException.class)
          .verify(Duration.ofSeconds(10));
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
