package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;

import io.rsocket.DuplexConnection;
import io.rsocket.transport.ClientTransport;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.annotations.Service;
import io.scalecube.services.annotations.ServiceMethod;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.routing.StaticAddressRouter;
import io.scalecube.services.transport.api.DataCodec;
import io.scalecube.services.transport.api.HeadersCodec;
import io.scalecube.services.transport.rsocket.RSocketClientTransport;
import io.scalecube.services.transport.rsocket.RSocketClientTransportFactory;
import io.scalecube.services.transport.rsocket.RSocketServiceTransport;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;
import java.util.concurrent.locks.LockSupport;
import java.util.function.BooleanSupplier;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.publisher.Mono;
import reactor.netty.resources.LoopResources;

/** Every connection the client transport opens must be tracked, so close() can dispose it. */
class RSocketClientTransportConnectionTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(5);
  private static final ServiceMessage REQUEST =
      ServiceMessage.builder().qualifier("v1/conn/hello").build();

  private final List<DuplexConnection> connections = new CopyOnWriteArrayList<>();

  private Microservices server;
  private LoopResources loopResources;
  private RSocketClientTransport transport;
  private ServiceReference serviceReference;

  @BeforeEach
  void setUp() {
    server =
        Microservices.start(
            new Context().transport(RSocketServiceTransport::new).services(new HelloServiceImpl()));

    loopResources = LoopResources.create("client-transport-test", 1, true);
    final var factory = RSocketClientTransportFactory.websocket().apply(loopResources);
    transport =
        new RSocketClientTransport(
            HeadersCodec.DEFAULT_INSTANCE,
            DataCodec.getAllInstances(),
            address -> recording(factory.clientTransport(address)),
            null,
            null,
            0,
            0);

    serviceReference =
        StaticAddressRouter.forService(server.serviceAddress(), "app-service")
            .build()
            .route(null, REQUEST)
            .orElseThrow();
  }

  @AfterEach
  void tearDown() {
    transport.close();
    server.close();
    loopResources.dispose();
  }

  @Test
  void staleChannelDoesNotOpenUntrackedConnection() {
    final var channel = transport.create(serviceReference);
    channel.requestResponse(REQUEST).block(TIMEOUT);
    assertEquals(1, connections.size(), "connections opened");

    // connection drops while a caller still holds the channel
    connections.get(0).dispose();
    Mono.delay(Duration.ofMillis(500)).block();

    channel.requestResponse(REQUEST).onErrorResume(ex -> Mono.empty()).block(TIMEOUT);
    assertEquals(1, connections.size(), "stale channel must not reconnect on its own");

    // a new channel reconnects through the cache as usual
    transport.create(serviceReference).requestResponse(REQUEST).block(TIMEOUT);
    assertEquals(2, connections.size(), "connections opened");

    transport.close();
    await(() -> connections.stream().allMatch(DuplexConnection::isDisposed));
  }

  private ClientTransport recording(ClientTransport clientTransport) {
    return () -> clientTransport.connect().doOnNext(connections::add);
  }

  private static void await(BooleanSupplier condition) {
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError("Condition not met within " + TIMEOUT);
      }
      LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
    }
  }

  @Service("v1/conn")
  public interface HelloService {

    @ServiceMethod
    Mono<String> hello();
  }

  public static class HelloServiceImpl implements HelloService {

    @Override
    public Mono<String> hello() {
      return Mono.just("hi");
    }
  }
}
