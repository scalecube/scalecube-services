package io.scalecube.services;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.services.sut.QuoteService;
import io.scalecube.services.sut.SimpleQuoteService;
import java.time.Duration;
import java.util.List;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import reactor.core.Disposable;

public class StreamingServiceTest extends BaseTest {

  private static Microservices gateway;
  private static Microservices node;

  /** Setup. */
  @BeforeAll
  public static void setup() {
    gateway = Microservices.builder().startAwait();

    node =
        Microservices.builder()
            .discovery(options -> options.seeds(gateway.discovery().address()))
            .services(new SimpleQuoteService())
            .startAwait();
  }

  @Test
  public void test_quotes() throws InterruptedException {

    CountDownLatch latch = new CountDownLatch(3);
    Disposable sub =
        node.call()
            .create()
            .api(QuoteService.class)
            .quotes()
            .subscribe(
                onNext -> {
                  System.out.println("test_quotes: " + onNext);
                  latch.countDown();
                });
    latch.await(4, TimeUnit.SECONDS);
    sub.dispose();
    assertTrue(latch.getCount() == 0);
  }

  @Test
  public void test_local_quotes_service() {

    QuoteService service = node.call().create().api(QuoteService.class);

    int expected = 3;
    List<String> list = service.quotes().take(Duration.ofSeconds(4)).collectList().block();

    assertEquals(expected, list.size());
  }

  @Test
  public void test_remote_quotes_service() throws InterruptedException {

    CountDownLatch latch1 = new CountDownLatch(3);
    CountDownLatch latch2 = new CountDownLatch(3);

    QuoteService service = gateway.call().create().api(QuoteService.class);

    Disposable sub1 =
        service
            .snapshot(3)
            .subscribe(
                onNext -> {
                  latch1.countDown();
                });

    Disposable sub2 =
        service
            .snapshot(3)
            .subscribe(
                onNext -> {
                  latch2.countDown();
                });

    latch1.await(5, TimeUnit.SECONDS);
    latch2.await(5, TimeUnit.SECONDS);

    assertTrue(latch1.getCount() == 0);
    assertTrue(latch2.getCount() == 0);
  }

  @Test
  public void test_quotes_batch() throws InterruptedException {
    int streamBound = 1000;

    QuoteService service = gateway.call().create().api(QuoteService.class);
    CountDownLatch latch1 = new CountDownLatch(streamBound);

    final Disposable sub1 = service.snapshot(streamBound).subscribe(onNext -> latch1.countDown());

    latch1.await(15, TimeUnit.SECONDS);
    System.out.println("Curr value received: " + latch1.getCount());
    assertEquals(0, latch1.getCount());
    sub1.dispose();
  }

  @Test
  public void test_call_quotes_snapshot() {
    int batchSize = 1000;

    ServiceCall serviceCall = gateway.call().create();

    ServiceMessage message =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "snapshot").data(batchSize).build();

    List<ServiceMessage> serviceMessages =
        serviceCall.requestMany(message).take(Duration.ofSeconds(5)).collectList().block();

    assertEquals(batchSize, serviceMessages.size());
  }

  @Test
  public void test_just_once() {
    QuoteService service = gateway.call().create().api(QuoteService.class);
    assertEquals("1", service.justOne().block(Duration.ofSeconds(2)));
  }

  @Test
  public void test_just_one_message() {

    ServiceCall service = gateway.call().create();

    ServiceMessage justOne =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "justOne").build();

    ServiceMessage message =
        service.requestOne(justOne, String.class).timeout(Duration.ofSeconds(3)).block();

    assertNotNull(message);
    assertEquals("1", message.<String>data());
  }

  @Test
  public void test_scheduled_messages() {
    ServiceCall serviceCall = gateway.call().create();

    ServiceMessage scheduled =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "scheduled").data(1000).build();

    int expected = 3;
    List<ServiceMessage> list =
        serviceCall.requestMany(scheduled).take(Duration.ofSeconds(4)).collectList().block();

    assertEquals(expected, list.size());
  }

  @Test
  public void test_unknown_method() {

    ServiceCall service = gateway.call().create();

    ServiceMessage scheduled =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "unknonwn").build();
    try {
      service.requestMany(scheduled).blockFirst(Duration.ofSeconds(3));
      fail("Expected no-reachable-service-exception");
    } catch (Exception ex) {
      assertTrue(ex.getMessage().contains("No reachable member with such service"));
    }
  }

  @Test
  public void test_snapshot_completes() {
    int batchSize = 1000;

    ServiceCall serviceCall = gateway.call().create();

    ServiceMessage message =
        ServiceMessage.builder().qualifier(QuoteService.NAME, "snapshot").data(batchSize).build();

    List<ServiceMessage> serviceMessages =
        serviceCall.requestMany(message).timeout(Duration.ofSeconds(5)).collectList().block();

    assertEquals(batchSize, serviceMessages.size());
  }
}
