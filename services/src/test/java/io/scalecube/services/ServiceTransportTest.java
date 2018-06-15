package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.exceptions.ConnectionClosedException;
import io.scalecube.services.streaming.QuoteService;
import io.scalecube.services.streaming.SimpleQuoteService;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.Disposable;

public class ServiceTransportTest {

  private static AtomicInteger port = new AtomicInteger(6000);

  private static final ServiceMessage JUST_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justNever").build();
  private static final ServiceMessage JUST_MANY_NEVER =
      ServiceMessage.builder().qualifier(QuoteService.NAME, "justManyNever").build();

  private Microservices gateway;
  private Microservices serviceNode;

  @BeforeEach
  public void setUp() {
    gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .startAwait();

    serviceNode = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();
  }

  @AfterEach
  public void cleanUp() {
    if (gateway != null) {
      try {
        gateway.shutdown();
      } catch (Throwable ignore) {
      }
    }
    if (serviceNode != null) {
      try {
        serviceNode.shutdown();
      } catch (Throwable ignore) {
      }
    }
  }

  @Test
  public void test_remote_node_died_mono() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call().create();
    sub1.set(serviceCall.requestOne(JUST_NEVER).log("test_remote_node_died_mono")
        .doOnError(exceptionHolder::set)
        .subscribe());

    gateway.cluster().listenMembership()
        .filter(MembershipEvent::isRemoved)
        .subscribe(onNext -> latch1.countDown());

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
  public void test_remote_node_died_flux() throws Exception {
    int batchSize = 1;

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    AtomicReference<Throwable> exceptionHolder = new AtomicReference<>(null);

    ServiceCall serviceCall = gateway.call().create();
    sub1.set(serviceCall.requestMany(JUST_MANY_NEVER).log("test_remote_node_died_flux")
        .doOnError(exceptionHolder::set)
        .subscribe());

    gateway.cluster().listenMembership()
        .filter(MembershipEvent::isRemoved)
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
