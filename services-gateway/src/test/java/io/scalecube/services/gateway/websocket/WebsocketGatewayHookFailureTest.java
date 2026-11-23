package io.scalecube.services.gateway.websocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;

import io.netty.buffer.ByteBuf;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.gateway.EchoService;
import io.scalecube.services.gateway.EchoServiceImpl;
import io.scalecube.services.gateway.GatewaySession;
import io.scalecube.services.gateway.GatewaySessionHandler;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.netty.http.client.HttpClient;

/** Buffers handed to a gateway handler hook that throws must still be released. */
class WebsocketGatewayHookFailureTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final String REQUEST =
      "{\"q\":\"" + EchoService.class.getName() + "/echo\",\"sid\":1,\"d\":\"hello\"}";

  private Microservices gateway;

  @AfterEach
  void afterEach() {
    if (gateway != null) {
      gateway.close();
    }
  }

  @Test
  void failingOnRequestReleasesFrame() {
    final List<ByteBuf> buffers = new CopyOnWriteArrayList<>();
    startGateway(
        new GatewaySessionHandler() {
          @Override
          public reactor.util.context.Context onRequest(
              GatewaySession session, ByteBuf byteBuf, reactor.util.context.Context context) {
            buffers.add(byteBuf);
            throw new IllegalStateException("onRequest failed");
          }
        });

    send(REQUEST);

    awaitReleased(buffers);
  }

  @Test
  void failingOnResponseReleasesFrame() {
    final List<ByteBuf> buffers = new CopyOnWriteArrayList<>();
    startGateway(
        new GatewaySessionHandler() {
          @Override
          public void onResponse(
              GatewaySession session,
              ByteBuf byteBuf,
              ServiceMessage response,
              reactor.util.context.Context context) {
            buffers.add(byteBuf);
            throw new IllegalStateException("onResponse failed");
          }
        });

    send(REQUEST);

    awaitReleased(buffers);
  }

  @Test
  void failingOnRequestAndOnErrorKeepSessionReading() {
    final List<ByteBuf> buffers = new CopyOnWriteArrayList<>();
    final AtomicInteger sessionErrors = new AtomicInteger();
    startGateway(
        new GatewaySessionHandler() {
          @Override
          public reactor.util.context.Context onRequest(
              GatewaySession session, ByteBuf byteBuf, reactor.util.context.Context context) {
            buffers.add(byteBuf);
            throw new IllegalStateException("onRequest failed");
          }

          @Override
          public void onError(
              GatewaySession session, Throwable throwable, reactor.util.context.Context context) {
            throw new IllegalStateException("onError failed");
          }

          @Override
          public void onSessionError(GatewaySession session, Throwable throwable) {
            sessionErrors.incrementAndGet();
          }
        });

    send(REQUEST, REQUEST);

    awaitReleased(buffers);
    assertEquals(2, buffers.size(), "frames reaching onRequest");
    assertEquals(0, sessionErrors.get(), "onSessionError calls");
  }

  private void startGateway(GatewaySessionHandler handler) {
    gateway =
        Microservices.start(
            new Context()
                .gateway(() -> WebsocketGateway.builder().id("WS").gatewayHandler(handler).build())
                .services(new EchoServiceImpl()));
  }

  private void send(String... frames) {
    final Address address = gateway.gateway("WS").address();
    HttpClient.create()
        .websocket()
        .uri("ws://" + address.host() + ":" + address.port() + "/")
        .handle(
            (in, out) ->
                out.sendString(Flux.just(frames))
                    .then()
                    // keep the connection open for a while so the gateway handles the frame
                    .then(Mono.delay(Duration.ofMillis(500)))
                    .then())
        .then()
        .block(TIMEOUT);
  }

  private static void awaitReleased(List<ByteBuf> buffers) {
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (buffers.isEmpty() || buffers.stream().anyMatch(b -> b.refCnt() > 0)) {
      if (System.nanoTime() > deadline) {
        assertFalse(buffers.isEmpty(), "hook was not called");
        throw new AssertionError(
            "Buffers not released: "
                + buffers.stream().map(b -> "refCnt=" + b.refCnt()).toList());
      }
      LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
    }
  }
}
