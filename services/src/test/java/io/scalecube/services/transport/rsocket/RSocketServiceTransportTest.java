package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.Address;
import io.scalecube.services.Microservices;
import io.scalecube.services.Microservices.Context;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.sut.QuoteService;
import io.scalecube.services.sut.SimpleQuoteService;
import io.scalecube.transport.netty.websocket.WebsocketTransportFactory;
import java.time.Duration;
import java.util.Optional;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.concurrent.locks.LockSupport;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

public class RSocketServiceTransportTest {

  private static final ServiceMessage JUST_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justNever").build();
  private static final ServiceMessage JUST_MANY_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justManyNever").build();
  private static final ServiceMessage ONLY_ONE_AND_THEN_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "onlyOneAndThenNever").build();
  public static final Duration TIMEOUT = Duration.ofSeconds(6);

  private Microservices gateway;
  private Microservices serviceNode;

  @BeforeEach
  public void setUp() {
    gateway =
        Microservices.start(
            new Context()
                .discovery(
                    endpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(endpoint)))
                .transport(RSocketServiceTransport::new));

    final Address gatewayAddress = this.gateway.discoveryAddress();

    serviceNode =
        Microservices.start(
            new Context()
                .discovery(
                    serviceEndpoint ->
                        new ScalecubeServiceDiscovery()
                            .transport(cfg -> cfg.transportFactory(new WebsocketTransportFactory()))
                            .options(opts -> opts.metadata(serviceEndpoint))
                            .membership(cfg -> cfg.seedMembers(gatewayAddress.toString())))
                .transport(RSocketServiceTransport::new)
                .services(new SimpleQuoteService()));
  }

  @AfterEach
  public void cleanUp() {
    Optional.ofNullable(gateway).ifPresent(Microservices::close);
    Optional.ofNullable(serviceNode).ifPresent(Microservices::close);
  }

  @Test
  public void test_closing_node_drops_its_accepted_connections() {
    awaitDiscovered(serviceNode);
    final ServiceCall serviceCall = gateway.call();
    // establishes the gateway -> serviceNode connection
    serviceCall.api(QuoteService.class).justOne().block(TIMEOUT);

    final AtomicReference<Throwable> error = new AtomicReference<>();
    final Disposable subscription =
        serviceCall.requestMany(JUST_MANY_NEVER).doOnError(error::set).subscribe();

    // the node must close the connection itself; the event loop shutdown alone may leave the
    // socket open and this stream hanging
    serviceNode.close();

    await(() -> error.get() != null, "connection loss was not observed by the caller");
    assertEquals(ConnectionClosedException.class, error.get().getClass());
    assertTrue(subscription.isDisposed());
  }

  private void awaitDiscovered(Microservices node) {
    await(
        () -> gateway.serviceEndpoints().stream().anyMatch(e -> e.id().equals(node.id())),
        "gateway did not discover " + node.id());
  }

  private static void await(java.util.function.BooleanSupplier condition, String message) {
    final long deadline = System.nanoTime() + TIMEOUT.toNanos();
    while (!condition.getAsBoolean()) {
      if (System.nanoTime() > deadline) {
        throw new AssertionError(message + " within " + TIMEOUT);
      }
      LockSupport.parkNanos(Duration.ofMillis(10).toNanos());
    }
  }

  @Disabled
  @Test
  public void test_remote_node_died_mono_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call();
    sub1.set(serviceCall.requestOne(JUST_NEVER).doOnError(exceptionHolder::set).subscribe());

    gateway
        .listenDiscovery()
        .filter(ServiceDiscoveryEvent::isEndpointRemoved)
        .subscribe(onNext -> latch.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.close();

    if (!latch.await(20, TimeUnit.SECONDS)) {
      fail("latch.await");
    }

    TimeUnit.MILLISECONDS.sleep(1000);

    assertEquals(0, latch.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  @Disabled("https://github.com/scalecube/scalecube-services/issues/882")
  @Test
  public void test_remote_node_died_many_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call();
    sub1.set(serviceCall.requestMany(JUST_MANY_NEVER).doOnError(exceptionHolder::set).subscribe());

    gateway
        .listenDiscovery()
        .filter(ServiceDiscoveryEvent::isEndpointRemoved)
        .subscribe(onNext -> latch.countDown(), System.err::println);

    // service node goes down
    serviceNode.close();

    if (!latch.await(20, TimeUnit.SECONDS)) {
      fail("latch.await");
    }

    assertEquals(0, latch.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  @Disabled("https://github.com/scalecube/scalecube-services/issues/882")
  @Test
  public void test_remote_node_died_many_then_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call();
    sub1.set(
        serviceCall
            .requestMany(ONLY_ONE_AND_THEN_NEVER)
            .doOnError(exceptionHolder::set)
            .subscribe());

    gateway
        .listenDiscovery()
        .filter(ServiceDiscoveryEvent::isEndpointRemoved)
        .subscribe(onNext -> latch.countDown(), System.err::println);

    // service node goes down
    serviceNode.close();

    if (!latch.await(20, TimeUnit.SECONDS)) {
      fail("latch.await");
    }

    assertEquals(0, latch.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }
}
