package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.discovery.ScalecubeServiceDiscovery;
import io.scalecube.services.discovery.api.ServiceDiscoveryEvent;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.sut.QuoteService;
import io.scalecube.services.sut.SimpleQuoteService;
import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

public class ServiceTransportTest {

  private static final ServiceMessage JUST_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justNever").build();
  private static final ServiceMessage JUST_MANY_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justManyNever").build();
  private static final ServiceMessage ONLY_ONE_AND_THEN_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "onlyOneAndThenNever").build();

  private Microservices gateway;
  private Microservices serviceNode;

  @BeforeEach
  public void setUp() {
    gateway =
        Microservices.builder()
            .discovery(ScalecubeServiceDiscovery::new)
            .transport(ServiceTransports::rsocketServiceTransport)
            .startAwait();

    serviceNode =
        Microservices.builder()
            .discovery(
                serviceEndpoint ->
                    new ScalecubeServiceDiscovery(serviceEndpoint)
                        .options(
                            opts -> opts.seedMembers(gateway.discovery().address())))
            .transport(ServiceTransports::rsocketServiceTransport)
            .services(new SimpleQuoteService())
            .startAwait();
  }

  @AfterEach
  public void cleanUp() {
    if (gateway != null) {
      try {
        gateway.shutdown();
      } catch (Throwable ignore) {
        // no-op
      }
    }
    if (serviceNode != null) {
      try {
        serviceNode.shutdown();
      } catch (Throwable ignore) {
        // no-op
      }
    }
  }

  @Test
  public void test_remote_node_died_mono_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call();
    sub1.set(serviceCall.requestOne(JUST_NEVER).doOnError(exceptionHolder::set).subscribe());

    gateway
        .discovery()
        .listenDiscovery()
        .filter(ServiceDiscoveryEvent::isEndpointRemoved)
        .subscribe(onNext -> latch1.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.shutdown().block(Duration.ofSeconds(6));

    latch1.await(20, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(100);

    assertEquals(0, latch1.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  @Test
  public void test_remote_node_died_many_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call();
    sub1.set(serviceCall.requestMany(JUST_MANY_NEVER).doOnError(exceptionHolder::set).subscribe());

    gateway
        .discovery()
        .listenDiscovery()
        .filter(ServiceDiscoveryEvent::isEndpointRemoved)
        .subscribe(onNext -> latch1.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.shutdown().block(Duration.ofSeconds(6));

    latch1.await(20, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(100);

    assertEquals(0, latch1.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }

  @Test
  public void test_remote_node_died_many_then_never() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call();
    sub1.set(
        serviceCall
            .requestMany(ONLY_ONE_AND_THEN_NEVER)
            .doOnError(exceptionHolder::set)
            .subscribe());

    gateway
        .discovery()
        .listenDiscovery()
        .filter(ServiceDiscoveryEvent::isEndpointRemoved)
        .subscribe(onNext -> latch1.countDown(), System.err::println);

    // service node goes down
    TimeUnit.SECONDS.sleep(3);
    serviceNode.shutdown().block(Duration.ofSeconds(6));

    latch1.await(20, TimeUnit.SECONDS);
    TimeUnit.MILLISECONDS.sleep(100);

    assertEquals(0, latch1.getCount());
    assertEquals(ConnectionClosedException.class, exceptionHolder.get().getClass());
    assertTrue(sub1.get().isDisposed());
  }
}
