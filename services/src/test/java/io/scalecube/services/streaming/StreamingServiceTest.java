package io.scalecube.services.streaming;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.BaseTest;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.api.ServiceMessage;

import com.codahale.metrics.MetricRegistry;

import org.junit.jupiter.api.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import reactor.core.Disposable;

public class StreamingServiceTest extends BaseTest {

  private MetricRegistry registry = new MetricRegistry();
  private static AtomicInteger port = new AtomicInteger(6000);

  @Test
  public void test_quotes() throws InterruptedException {
    QuoteService service = new SimpleQuoteService();
    CountDownLatch latch = new CountDownLatch(3);
    Disposable sub = service.quotes().subscribe(onNext -> {
      System.out.println("test_quotes: " + onNext);
      latch.countDown();
    });
    latch.await(4, TimeUnit.SECONDS);
    sub.dispose();
    assertTrue(latch.getCount() == 0);
  }

  @Test
  public void test_local_quotes_service() {
    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new SimpleQuoteService())
        .startAwait();

    QuoteService service = node.call().create().api(QuoteService.class);

    int expected = 3;
    List<String> list =
        service.quotes().take(Duration.ofSeconds(4)).collectList().block();

    assertEquals(expected, list.size());

    node.shutdown();
  }

  @Test
  public void test_remote_quotes_service() throws InterruptedException {
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    QuoteService service = gateway.call().create().api(QuoteService.class);
    CountDownLatch latch1 = new CountDownLatch(3);
    CountDownLatch latch2 = new CountDownLatch(3);

    Disposable sub1 = service.quotes()
        .subscribe(onNext -> {
          System.out.println("test_remote_quotes_service-2: " + onNext);
          latch1.countDown();
        });

    Disposable sub2 = service.quotes()
        .subscribe(onNext -> {
          System.out.println("test_remote_quotes_service-10: " + onNext);
          latch2.countDown();
        });

    latch1.await(4, TimeUnit.SECONDS);
    latch2.await(4, TimeUnit.SECONDS);
    sub1.dispose();
    sub2.dispose();
    assertTrue(latch1.getCount() == 0);
    assertTrue(latch2.getCount() == 0);
    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_quotes_batch() throws InterruptedException {
    int streamBound = 1000;

    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet()).startAwait();
    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .metrics(registry)
        .startAwait();

    QuoteService service = gateway.call().create().api(QuoteService.class);
    CountDownLatch latch1 = new CountDownLatch(streamBound);

    Disposable sub1 = service.snapshot(streamBound)
        .subscribe(onNext -> latch1.countDown());

    latch1.await(15, TimeUnit.SECONDS);
    System.out.println("Curr value received: " + latch1.getCount());
    assertTrue(latch1.getCount() == 0);
    sub1.dispose();
    node.shutdown();
    gateway.shutdown();
  }

  @Test
  public void test_call_quotes_snapshot() {
    int batchSize = 1000;
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    ServiceCall serviceCall = gateway.call().create();

    ServiceMessage message =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "snapshot").data(batchSize).build();

    List<ServiceMessage> serviceMessages =
        serviceCall.requestMany(message).take(Duration.ofSeconds(5)).collectList().block();

    assertEquals(batchSize, serviceMessages.size());

    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_just_once() {
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    QuoteService service = gateway.call().create().api(QuoteService.class);

    assertEquals("1", service.justOne().block(Duration.ofSeconds(2)));

    gateway.shutdown();
    node.shutdown();

  }

  @Test
  public void test_just_one_message() {
    Microservices gateway = Microservices.builder().startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    Call service = gateway.call();

    ServiceMessage justOne = ServiceMessage.builder().qualifier(QuoteService.NAME, "justOne").build();

    ServiceMessage message =
        service.create().requestOne(justOne, String.class).timeout(Duration.ofSeconds(3)).block();

    assertNotNull(message);
    assertEquals("1", message.<String>data());

    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_scheduled_messages() {
    Microservices gateway = Microservices.builder().startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    ServiceCall serviceCall = gateway.call().create();

    ServiceMessage scheduled =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "scheduled").data(1000).build();

    int expected = 3;
    List<ServiceMessage> list =
        serviceCall.requestMany(scheduled).take(Duration.ofSeconds(4)).collectList().block();

    assertEquals(expected, list.size());

    node.shutdown();
    gateway.shutdown();
  }

  @Test
  public void test_unknown_method() {

    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .startAwait();
    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    Call service = gateway.call();

    ServiceMessage scheduled = ServiceMessage.builder()
        .qualifier(QuoteService.NAME, "unknonwn").build();
    try {
      service.create().requestMany(scheduled).blockFirst(Duration.ofSeconds(3));
      fail("Expected no-reachable-service-exception");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("No reachable member with such service"));
    }

    node.shutdown();
    gateway.shutdown();
  }

  @Test
  public void test_snapshot_completes() {
    int batchSize = 1000;
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .startAwait();

    ServiceCall serviceCall = gateway.call().create();

    ServiceMessage message =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "snapshot").data(batchSize).build();

    List<ServiceMessage> serviceMessages =
        serviceCall.requestMany(message).timeout(Duration.ofSeconds(5)).collectList().block();

    assertEquals(batchSize, serviceMessages.size());

    gateway.shutdown();
    node.shutdown();
  }
}
