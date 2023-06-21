package io.scalecube.services.gateway.websocket;

import io.netty.buffer.ByteBuf;
import io.scalecube.net.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.TestGatewaySessionHandler;
import io.scalecube.services.gateway.transport.GatewayClient;
import io.scalecube.services.gateway.transport.GatewayClientCodec;
import io.scalecube.services.gateway.transport.GatewayClientSettings;
import io.scalecube.services.gateway.transport.GatewayClientTransport;
import io.scalecube.services.gateway.transport.GatewayClientTransports;
import io.scalecube.services.gateway.transport.StaticAddressRouter;
import io.scalecube.services.gateway.transport.websocket.WebsocketGatewayClient;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;
import reactor.test.StepVerifier;

class WebsocketServerTest extends BaseTest {

  public static final GatewayClientCodec<ByteBuf> CLIENT_CODEC =
      GatewayClientTransports.WEBSOCKET_CLIENT_CODEC;

  private static Microservices gateway;
  private static Address gatewayAddress;
  private static GatewayClient client;
  private static LoopResources loopResources;

  @BeforeAll
  static void beforeAll() {
    loopResources = LoopResources.create("websocket-gateway-client");

    gateway =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .gateway(
                options -> new WebsocketGateway(options.id("WS"), new TestGatewaySessionHandler()))
            .transport(RSocketServiceTransport::new)
            .services(new TestServiceImpl())
            .startAwait();
    gatewayAddress = gateway.gateway("WS").address();
  }

  @AfterEach
  void afterEach() {
    final GatewayClient client = WebsocketServerTest.client;
    if (client != null) {
      client.close();
    }
  }

  @AfterAll
  static void afterAll() {
    final GatewayClient client = WebsocketServerTest.client;
    if (client != null) {
      client.close();
    }

    Mono.justOrEmpty(gateway).map(Microservices::shutdown).then().block();

    if (loopResources != null) {
      loopResources.disposeLater().block();
    }
  }

  @Test
  void testMessageSequence() {
    client =
        new WebsocketGatewayClient(
            GatewayClientSettings.builder().address(gatewayAddress).build(),
            CLIENT_CODEC,
            loopResources);

    ServiceCall serviceCall =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(gatewayAddress));

    int count = 1000;

    StepVerifier.create(serviceCall.api(TestService.class).many(count) /*.log("<<< ")*/)
        .expectNextSequence(IntStream.range(0, count).boxed().collect(Collectors.toList()))
        .expectComplete()
        .verify(Duration.ofSeconds(10));
  }

  @Service
  public interface TestService {

    @ServiceMethod("many")
    Flux<Integer> many(int count);
  }

  private static class TestServiceImpl implements TestService {

    @Override
    public Flux<Integer> many(int count) {
      return Flux.using(
          ReactiveAdapter::new,
          reactiveAdapter ->
              reactiveAdapter
                  .receive()
                  .take(count)
                  .cast(Integer.class)
                  .doOnSubscribe(
                      s ->
                          new Thread(
                                  () -> {
                                    for (int i = 0; ; ) {
                                      int r = (int) reactiveAdapter.requested(100);

                                      if (reactiveAdapter.isFastPath()) {
                                        try {
                                          if (reactiveAdapter.isDisposed()) {
                                            return;
                                          }
                                          reactiveAdapter.tryNext(i++);
                                          reactiveAdapter.incrementProduced();
                                        } catch (Throwable e) {
                                          reactiveAdapter.lastError(e);
                                          return;
                                        }
                                      } else if (r > 0) {
                                        try {
                                          if (reactiveAdapter.isDisposed()) {
                                            return;
                                          }
                                          reactiveAdapter.tryNext(i++);
                                          reactiveAdapter.incrementProduced();
                                        } catch (Throwable e) {
                                          reactiveAdapter.lastError(e);
                                          return;
                                        }

                                        reactiveAdapter.commitProduced();
                                      }
                                    }
                                  })
                              .start()),
          ReactiveAdapter::dispose);
    }
  }
}
