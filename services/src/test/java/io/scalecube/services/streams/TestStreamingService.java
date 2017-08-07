package io.scalecube.services.streams;

import static org.junit.Assert.*;

import io.scalecube.services.Microservices;

import org.junit.Test;

import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

public class TestStreamingService {

  @Test
  public void test_quotes() {

    QuoteService service = new SimpleQuoteService();
    service.quotes(2).subscribe(onNext -> {
      System.out.println("test_quotes: " + onNext);
    });

  }

  @Test
  public void test_local_quotes_service() {
    Microservices node = Microservices.builder().services(new SimpleQuoteService()).build();

    QuoteService service = node.proxy().api(QuoteService.class).create();

    service.quotes(2).subscribe(onNext -> {
      System.out.println("test_local_quotes_service: " + onNext);
    });
  }

  @Test
  public void test_remote_quotes_service() throws InterruptedException {
    Microservices gateway = Microservices.builder().build();

    Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    QuoteService service = gateway.proxy().api(QuoteService.class).create();
    CountDownLatch latch = new CountDownLatch(3);
    
    Subscription sub = service.quotes(2)
        .subscribe(onNext -> {
          System.out.println("test_remote_quotes_service: " + onNext);
          latch.countDown();
        });
    
    latch.await(4, TimeUnit.SECONDS);
    sub.unsubscribe();
    assertTrue(latch.getCount() == 0);
    System.out.println("done");
  }
}
