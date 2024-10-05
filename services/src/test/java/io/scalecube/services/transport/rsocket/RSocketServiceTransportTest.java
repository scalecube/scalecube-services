package io.scalecube.services.transport.rsocket;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.Address;
import io.scalecube.services.BaseTest;
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
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Disabled;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

public class RSocketServiceTransportTest extends BaseTest {

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
}
