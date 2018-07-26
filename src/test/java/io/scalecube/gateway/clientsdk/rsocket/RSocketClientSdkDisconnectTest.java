package io.scalecube.gateway.clientsdk.rsocket;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.core.StringEndsWith.endsWith;
import static org.hamcrest.core.StringStartsWith.startsWith;

import io.scalecube.gateway.clientsdk.Client;
import io.scalecube.gateway.clientsdk.ClientSettings;
import io.scalecube.gateway.clientsdk.codec.ClientMessageCodec;
import io.scalecube.gateway.clientsdk.exceptions.ConnectionClosedException;
import io.scalecube.gateway.examples.GreetingService;
import io.scalecube.gateway.examples.GreetingServiceImpl;
import io.scalecube.gateway.rsocket.websocket.RSocketWebsocketServer;
import io.scalecube.services.Microservices;
import io.scalecube.services.codec.DataCodec;
import io.scalecube.services.codec.HeadersCodec;

import reactor.core.publisher.Mono;
import reactor.ipc.netty.resources.LoopResources;
import reactor.test.StepVerifier;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;

class RSocketClientSdkDisconnectTest {

  private static final Duration SHUTDOWN_TIMEOUT = Duration.ofSeconds(3);

  private static final String JOHN = "John";
  private static final int RSOCKET_PORT = 8080;

  private RSocketWebsocketServer rsocketServer;
  private LoopResources clientLoopResources;
  private Microservices seed;

  private Client rsocketClient;

  @BeforeEach
  void startClient() {
    seed = Microservices
        .builder()
        .services(new GreetingServiceImpl())
        .startAwait();

    rsocketServer = new RSocketWebsocketServer(seed, RSOCKET_PORT);
    rsocketServer.start();
    clientLoopResources = LoopResources.create("eventLoop");

    ClientSettings settings = ClientSettings.builder().port(RSOCKET_PORT).build();

    ClientMessageCodec codec = new ClientMessageCodec(
        HeadersCodec.getInstance(settings.contentType()),
        DataCodec.getInstance(settings.contentType()));

    RSocketClientTransport clientTransport =
        new RSocketClientTransport(settings, codec, clientLoopResources);

    rsocketClient = new Client(clientTransport, codec);
  }

  @AfterEach
  void stopClient() {
    if (rsocketClient != null) {
      rsocketClient.close().block(SHUTDOWN_TIMEOUT);
    }
    if (rsocketServer != null) {
      rsocketServer.stop();
    }
    if (clientLoopResources != null) {
      clientLoopResources.disposeLater().block(SHUTDOWN_TIMEOUT);
    }
    if (seed != null) {
      seed.shutdown().block(SHUTDOWN_TIMEOUT);
    }
  }

  @Test
  void testServerDisconnection() {
    Duration shutdownAt = Duration.ofSeconds(1);

    StepVerifier.create(rsocketClient.forService(GreetingService.class).many(JOHN)
        .doOnSubscribe(subscription -> Mono.delay(shutdownAt)
            .doOnSuccess(aLong -> rsocketServer.stop())
            .subscribe()))
        .thenConsumeWhile(response -> {
          assertThat(response, startsWith("Greeting ("));
          assertThat(response, endsWith(") to: " + JOHN));
          return true;
        })
        .expectError(ConnectionClosedException.class)
        .verify(shutdownAt.plus(SHUTDOWN_TIMEOUT));
  }
}
