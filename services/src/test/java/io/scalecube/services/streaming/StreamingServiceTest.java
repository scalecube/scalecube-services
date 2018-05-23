package io.scalecube.services.streaming;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.cluster.membership.MembershipEvent;
import io.scalecube.services.BaseTest;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall.Call;
import io.scalecube.services.api.ServiceMessage;

import com.codahale.metrics.MetricRegistry;

import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.atomic.AtomicReference;

import reactor.core.Disposable;
import reactor.core.publisher.Flux;

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
  public void test_local_quotes_service() throws InterruptedException {
    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .services(new SimpleQuoteService())
        .build()
        .startAwait();

    QuoteService service = node.call().create().api(QuoteService.class);

    int expected = 3;
    List<String> list =
        service.quotes().timeout(Duration.ofSeconds(4)).take(expected).collectList().block();

    assertEquals(expected, list.size());

    node.shutdown();
  }

  @Test
  public void test_remote_quotes_service() throws InterruptedException {
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
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
        .discoveryPort(port.incrementAndGet()).build().startAwait();
    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .metrics(registry)
        .build()
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
  public void test_call_quotes_snapshot() throws InterruptedException {
    int batchSize = 1000;
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
        .startAwait();

    Call service = gateway.call();

    ServiceMessage message =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "snapshot").data(batchSize).build();

    List<ServiceMessage> serviceMessages =
        service.create().requestMany(message).timeout(Duration.ofSeconds(5)).collectList().block();

    assertEquals(batchSize, serviceMessages.size());

    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_just_once() {
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
        .startAwait();

    QuoteService service = gateway.call().create().api(QuoteService.class);

    assertEquals("1", service.justOne().block(Duration.ofSeconds(2)));

    gateway.shutdown();
    node.shutdown();

  }

  @Test
  public void test_just_one_message() throws InterruptedException {
    int batchSize = 1;
    Microservices gateway = Microservices.builder().build().startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
        .startAwait();

    Call service = gateway.call();

    ServiceMessage justOne = ServiceMessage.builder().qualifier(QuoteService.NAME, "justOne").build();

    List<ServiceMessage> list =
        Flux.from(service.create().requestOne(justOne)).timeout(Duration.ofSeconds(3)).collectList().block();

    assertEquals(1, list.size());

    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_scheduled_messages() throws InterruptedException {
    Microservices gateway = Microservices.builder().build().startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
        .startAwait();

    Call service = gateway.call();

    ServiceMessage scheduled =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "scheduled").data(1000).build();

    int expected = 3;
    List<ServiceMessage> list =
        service.create().requestMany(scheduled).timeout(Duration.ofSeconds(4)).take(expected).collectList().block();

    assertEquals(expected, list.size());

    node.shutdown();
    gateway.shutdown();
  }

  @Test
  public void test_unknown_method() throws InterruptedException {

    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();
    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
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
  public void test_remote_node_died() throws InterruptedException {
    int batchSize = 1;
    Microservices gateway = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .build()
        .startAwait();

    Microservices node = Microservices.builder()
        .discoveryPort(port.incrementAndGet())
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .build()
        .startAwait();

    Call service = gateway.call();

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Disposable> sub1 = new AtomicReference<>(null);
    ServiceMessage justOne = ServiceMessage.builder().qualifier(QuoteService.NAME, "justOne").build();

    sub1.set(Flux.from(service.create().requestMany(justOne)).subscribe(System.out::println));

    gateway.cluster().listenMembership()
        .filter(MembershipEvent::isRemoved)
        .subscribe(onNext -> latch1.countDown());

    node.cluster().shutdown();

    latch1.await(20, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertTrue(latch1.getCount() == 0);
    assertTrue(sub1.get().isDisposed());
    gateway.shutdown();
  }
}
