package io.scalecube.services.streams;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices;

import org.junit.Test;

import rx.Subscription;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestStreamingService {

  @Test
  public void test_quotes() throws InterruptedException {

    QuoteService service = new SimpleQuoteService();
    CountDownLatch latch = new CountDownLatch(3);
    Subscription sub = service.quotes(2).subscribe(onNext -> {
      System.out.println("test_quotes: " + onNext);
      latch.countDown();
    });
    latch.await(1, TimeUnit.SECONDS);
    sub.unsubscribe();
    assertTrue(latch.getCount() <= 0);
  }

  @Test
  public void test_local_quotes_service() throws InterruptedException {
    Microservices node = Microservices.builder().services(new SimpleQuoteService()).build();

    QuoteService service = node.proxy().api(QuoteService.class).create();

    CountDownLatch latch = new CountDownLatch(3);
    Subscription sub = service.quotes(2).subscribe(onNext -> {
      System.out.println("test_local_quotes_service: " + onNext);
      latch.countDown();
    });
    latch.await(1, TimeUnit.SECONDS);
    
    sub.unsubscribe();
    assertTrue(latch.getCount() <= 0);
    node.shutdown();
  }

  @Test
  public void test_remote_quotes_service() throws InterruptedException {
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    QuoteService service = gateway.proxy().api(QuoteService.class).create();
    CountDownLatch latch = new CountDownLatch(3);

    Subscription sub = service.quotes(2)
        .subscribe(onNext -> {
          System.out.println("test_remote_quotes_service: " + onNext);
          latch.countDown();
        });

    latch.await(1, TimeUnit.SECONDS);
    sub.unsubscribe();
    assertTrue(latch.getCount() <= 0);
    System.out.println("done");
    gateway.shutdown();
    node.shutdown();
  }
}
