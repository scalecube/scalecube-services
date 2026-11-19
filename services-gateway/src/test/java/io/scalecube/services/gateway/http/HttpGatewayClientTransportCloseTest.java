package io.scalecube.services.gateway.http;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.netty.channel.Channel;
import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.gateway.client.http.HttpGatewayClientTransport;
import io.scalecube.services.routing.StaticAddressRouter;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

/** Closing the client transport must close its pooled keep-alive connections. */
class HttpGatewayClientTransportCloseTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);

  private Microservices gateway;
  private LoopResources loopResources;

  @BeforeEach
  void setUp() {
    gateway =
        Microservices.start(
            new Context()
                .gateway(() -> HttpGateway.builder().id("HTTP").build())
                .services(new EchoServiceImpl()));
    // supplied by the caller, so the transport doesn't dispose it on close
    loopResources = LoopResources.create("http-client-close-test", 1, true);
  }

  @AfterEach
  void tearDown() {
    gateway.close();
    loopResources.dispose();
  }

  @Test
  void closeReleasesPooledConnections() {
    final List<Channel> channels = new CopyOnWriteArrayList<>();
    final Address address = gateway.gateway("HTTP").address();

    final var transport =
        HttpGatewayClientTransport.builder()
            .address(address)
            .loopResources(loopResources)
            .httpClient(client -> client.doOnConnected(c -> channels.add(c.channel())))
            .build();

    try (ServiceCall serviceCall =
        new ServiceCall()
            .transport(transport)
            .router(StaticAddressRouter.forService(address, "app-service").build())) {
      assertEquals("hello", serviceCall.api(EchoService.class).echo("hello").block(TIMEOUT));
    }

    // the pooled socket itself, not reactor-netty's per-request handle, which is released early
    assertEquals(1, channels.size(), "connections opened");
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (channels.get(0).isOpen()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Pooled connection still open after transport close");
      }
      LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
    }
  }

  @Service("v1/close-test")
  public interface EchoService {

    @ServiceMethod
    Mono<String> echo(String request);
  }

  public static class EchoServiceImpl implements EchoService {

    @Override
    public Mono<String> echo(String request) {
      return Mono.just(request);
    }
  }
}
