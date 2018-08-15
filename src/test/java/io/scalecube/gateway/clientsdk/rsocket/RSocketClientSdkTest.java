package io.scalecube.gateway.clientsdk.rsocket;

import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingResponse;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketGateway;
import io.scalecube.services.Microservices;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;
import io.scalecube.services.gateway.GatewayConfig;
import java.time.Duration;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;
import reactor.test.StepVerifier;

class RSocketClientSdkTest {

  private static final GatewayConfig gatewayConfig =
      GatewayConfig.builder(RSocketWebsocketGateway.class).build();
  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(3);

  private static final String JOHN = "John";

  private static LoopResources clientLoopResources;
  private static Microservices seed;

  private Client rsocketClient;

  @BeforeAll
  static void startServer() {
    seed =
        Microservices.builder()
            .services(new GreetingServiceImpl())
            .gateway(gatewayConfig)
            .startAwait();

    clientLoopResources = LoopResources.create("eventLoop");
  }

  @AfterAll
  static void stopServers() {
    clientLoopResources.disposeLater().block(SHUTDOWN_TIMEOUT);
    seed.shutdown().block(SHUTDOWN_TIMEOUT);
  }

  @BeforeEach
  void startClient() {
    int gatewayPort = seed.gatewayAddress(gatewayConfig.gatewayClass()).getPort();
    ClientSettings settings = ClientSettings.builder().port(gatewayPort).build();
    ClientMessageCodec codec = new ClientMessageCodec(
        HeadersCodec.getInstance(settings.contentType()),
        DataCodec.getInstance(settings.contentType()));
    rsocketClient = new Client(new RSocketClientTransport(settings, codec, clientLoopResources), codec);
  }

  @AfterEach
  void stopClient() {
    if (rsocketClient != null) {
      rsocketClient.close().block(SHUTDOWN_TIMEOUT);
    }
  }

  @Test
  void testOneScalar() {
    Mono<String> john = rsocketClient
        .forService(GreetingService.class)
        .one(JOHN);

    StepVerifier.create(john)
        .assertNext(n -> Assertions.assertTrue(n.contains(JOHN)))
        .expectComplete()
        .verify();
  }

  @Test
  void testStreamScalar() {
    int cnt = 5;
    Flux<String> johnnys = rsocketClient
        .forService(GreetingService.class)
        .many(JOHN)
        .take(cnt);
    StepVerifier.create(johnnys)
        .expectNextCount(cnt)
        .expectComplete()
        .verify();
  }

  @Test
  void testOnePojo() {
    int cnt = 5;

    Mono<GreetingResponse> johnnys = rsocketClient
        .forService(GreetingService.class)
        .pojoOne(new GreetingRequest().setText(JOHN));

    StepVerifier.create(johnnys)
        .assertNext(n -> Assertions.assertEquals("Echo:" + JOHN, n.getText()))
        .expectComplete()
        .verify();
  }

  @Test
  void testStreamPojo() {
    int cnt = 5;

    Flux<GreetingResponse> johnnys = rsocketClient
        .forService(GreetingService.class)
        .pojoMany(new GreetingRequest().setText(JOHN))
        .take(cnt);

    StepVerifier.create(johnnys)
        .assertNext(n -> Assertions.assertTrue(n.getText().contains(JOHN)))
        .expectNextCount(cnt - 1 /* minus previous check */)
        .expectComplete()
        .verify();
  }

  @Test
  void testFailingStream() {
    Flux failing = rsocketClient.forService(GreetingService.class)
        .failingMany(JOHN);

    StepVerifier.create(failing)
        .expectNextCount(2)
        .expectError()
        .verify();
  }
}
