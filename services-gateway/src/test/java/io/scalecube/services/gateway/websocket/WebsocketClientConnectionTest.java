package io.scalecube.services.gateway.websocket;

import static io.scalecube.services.gateway.TestUtils.TIMEOUT;
import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.buffer.ByteBuf;
import io.netty.channel.ChannelHandlerContext;
import io.netty.channel.ChannelInboundHandlerAdapter;
import io.netty.handler.codec.http.websocketx.PongWebSocketFrame;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.gateway.BaseTest;
import io.scalecube.services.gateway.TestGatewaySessionHandler;
import io.scalecube.services.gateway.TestService;
import io.scalecube.services.gateway.TestServiceImpl;
import io.scalecube.services.gateway.TestUtils;
import io.scalecube.services.gateway.client.GatewayClient;
import io.scalecube.services.gateway.client.GatewayClientCodec;
import io.scalecube.services.gateway.client.GatewayClientSettings;
import io.scalecube.services.gateway.client.GatewayClientTransport;
import io.scalecube.services.gateway.client.GatewayClientTransports;
import io.scalecube.services.gateway.client.StaticAddressRouter;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClient;
import io.scalecube.services.gateway.client.websocket.WebsocketGatewayClientSession;
import io.scalecube.services.gateway.ws.WebsocketGateway;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.io.IOException;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.time.Duration;
import java.util.Collections;
import java.util.UUID;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.Connection;
import reactor.test.StepVerifier;

class WebsocketClientConnectionTest extends BaseTest {

  public static final GatewayClientCodec<ByteBuf> CLIENT_CODEC =
      GatewayClientTransports.WEBSOCKET_CLIENT_CODEC;
  private static final AtomicInteger onCloseCounter = new AtomicInteger();
  private Microservices gateway;
  private Address gatewayAddress;
  private Microservices service;
  private GatewayClient client;
  private TestGatewaySessionHandler sessionEventHandler;

  @BeforeEach
  void beforEach() {
    this.sessionEventHandler = new TestGatewaySessionHandler();
    gateway =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery()
                        .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                        .options(opts -> opts.metadata(serviceEndpoint)))
            .transport(RSocketServiceTransport::new)
            .gateway(options -> new WebsocketGateway(options.id("WS"), sessionEventHandler))
            .startAwait();

    gatewayAddress = gateway.gateway("WS").address();

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
            .services(new TestServiceImpl(onCloseCounter::incrementAndGet))
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
        new WebsocketGatewayClient(
            GatewayClientSettings.builder().address(gatewayAddress).build(), CLIENT_CODEC);

    ServiceCall serviceCall =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(gatewayAddress));

    StepVerifier.create(serviceCall.api(TestService.class).manyNever().log("<<< "))
        .thenAwait(Duration.ofSeconds(5))
        .then(() -> client.close())
        .then(() -> client.onClose().block())
        .expectError(IOException.class)
        .verify(Duration.ofSeconds(10));

    TestUtils.await(() -> onCloseCounter.get() == 1).block(TIMEOUT);
    assertEquals(1, onCloseCounter.get());
  }

  @Test
  public void testCallRepeatedlyByInvalidAddress() {
    Address invalidAddress = Address.create("localhost", 5050);

    client =
        new WebsocketGatewayClient(
            GatewayClientSettings.builder().address(invalidAddress).build(), CLIENT_CODEC);

    ServiceCall serviceCall =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(invalidAddress));

    for (int i = 0; i < 100; i++) {
      StepVerifier.create(serviceCall.api(TestService.class).manyNever().log("<<< "))
          .thenAwait(Duration.ofSeconds(1))
          .expectError(IOException.class)
          .verify(Duration.ofSeconds(10));
    }
  }

  @Test
  public void testHandlerEvents() throws InterruptedException {
    // Test Connect
    client =
        new WebsocketGatewayClient(
            GatewayClientSettings.builder().address(gatewayAddress).build(), CLIENT_CODEC);

    TestService service =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(gatewayAddress))
            .api(TestService.class);

    service.one("one").block(TIMEOUT);
    sessionEventHandler.connLatch.await(3, TimeUnit.SECONDS);
    Assertions.assertEquals(0, sessionEventHandler.connLatch.getCount());

    sessionEventHandler.msgLatch.await(3, TimeUnit.SECONDS);
    Assertions.assertEquals(0, sessionEventHandler.msgLatch.getCount());

    client.close();
    sessionEventHandler.disconnLatch.await(3, TimeUnit.SECONDS);
    Assertions.assertEquals(0, sessionEventHandler.disconnLatch.getCount());
  }

  @Test
  void testKeepalive()
      throws InterruptedException,
          NoSuchFieldException,
          IllegalAccessException,
          NoSuchMethodException,
          InvocationTargetException {

    int expectedKeepalives = 3;
    Duration keepAliveInterval = Duration.ofSeconds(1);
    CountDownLatch keepaliveLatch = new CountDownLatch(expectedKeepalives);
    client =
        new WebsocketGatewayClient(
            GatewayClientSettings.builder()
                .address(gatewayAddress)
                .keepAliveInterval(keepAliveInterval)
                .build(),
            CLIENT_CODEC);

    Method getOrConnect = WebsocketGatewayClient.class.getDeclaredMethod("getOrConnect");
    getOrConnect.setAccessible(true);
    //noinspection unchecked
    WebsocketGatewayClientSession session =
        ((Mono<WebsocketGatewayClientSession>) getOrConnect.invoke(client)).block(TIMEOUT);
    Field connectionField = WebsocketGatewayClientSession.class.getDeclaredField("connection");
    connectionField.setAccessible(true);
    Connection connection = (Connection) connectionField.get(session);
    connection.addHandler(
        new ChannelInboundHandlerAdapter() {
          @Override
          public void channelRead(ChannelHandlerContext ctx, Object msg) throws Exception {
            if (msg instanceof PongWebSocketFrame) {
              ((PongWebSocketFrame) msg).release();
              keepaliveLatch.countDown();
            } else {
              super.channelRead(ctx, msg);
            }
          }
        });

    keepaliveLatch.await(
        keepAliveInterval.toMillis() * (expectedKeepalives + 1), TimeUnit.MILLISECONDS);

    assertEquals(0, keepaliveLatch.getCount());
  }

  @Test
  void testClientSettingsHeaders() {
    String headerKey = "secret-token";
    String headerValue = UUID.randomUUID().toString();
    client =
        new WebsocketGatewayClient(
            GatewayClientSettings.builder()
                .address(gatewayAddress)
                .headers(Collections.singletonMap(headerKey, headerValue))
                .build(),
            CLIENT_CODEC);
    TestService service =
        new ServiceCall()
            .transport(new GatewayClientTransport(client))
            .router(new StaticAddressRouter(gatewayAddress))
            .api(TestService.class);

    StepVerifier.create(
            service.one("one").then(Mono.fromCallable(() -> sessionEventHandler.lastSession())))
        .assertNext(session -> assertEquals(headerValue, session.headers().get(headerKey)))
        .expectComplete()
        .verify(TIMEOUT);
  }
}
