package io.scalecube.services.streams;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices;

import org.junit.Test;

import rx.Subscription;
import rx.observers.Observers;
import rx.observers.Subscribers;
import rx.schedulers.Schedulers;

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
    CountDownLatch latch1 = new CountDownLatch(3);
    CountDownLatch latch2 = new CountDownLatch(3);
   
    Subscription sub1 = service.quotes(2)
        .subscribe(onNext -> {
          System.out.println("test_remote_quotes_service-2: " + onNext);
          latch1.countDown();
        });

    Subscription sub2 = service.quotes(10)
        .subscribe(onNext -> {
          System.out.println("test_remote_quotes_service-10: " + onNext);
          latch2.countDown();
        });
    
    latch1.await(1, TimeUnit.SECONDS);
    latch2.await(1, TimeUnit.SECONDS);
    sub1.unsubscribe();
    sub2.unsubscribe();
    assertTrue(latch1.getCount() <= 0);
    assertTrue(latch2.getCount() <= 0);
    System.out.println("done");
    gateway.shutdown();
    node.shutdown();
  }
  
  @Test
  public void test_quotes_snapshot() throws InterruptedException {
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    
    QuoteService service = gateway.proxy().api(QuoteService.class).create();
    CountDownLatch latch1 = new CountDownLatch(100_000);
  
    Subscription sub1 = service.snapshoot(100_000)
        .onBackpressureBuffer()
        .serialize()
        .subscribeOn(Schedulers.computation())
        .subscribe(onNext -> {
          latch1.countDown();
        });

    latch1.await(30, TimeUnit.SECONDS);
    System.out.println("done: " + latch1.getCount());
    assertTrue(latch1.getCount() <= 0);
    
    sub1.unsubscribe();
    gateway.shutdown();
    node.shutdown();
  }
}
