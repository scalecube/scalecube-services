package io.scalecube.services.streaming;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;

import org.junit.Test;

import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
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
    int batchSize = 1_000_000;

    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    QuoteService service = gateway.proxy().api(QuoteService.class).create();

    CountDownLatch latch1 = new CountDownLatch(batchSize);

    Subscription sub1 = service.snapshoot(batchSize)
        .subscribeOn(Schedulers.from(Executors.newCachedThreadPool()))
        .serialize()
        .subscribe(onNext -> latch1.countDown());

    long start = System.currentTimeMillis();
    latch1.await(batchSize / 60_000, TimeUnit.SECONDS);
    long end = (System.currentTimeMillis() - start);

    System.out.println("TIME IS UP! - recived batch (size: "
        + (batchSize - latch1.getCount()) + "/" + batchSize + ") in "
        + (System.currentTimeMillis() - start) + "ms: "
        + "rate of :" + batchSize / (end / 1000) + " events/sec ");

    assertTrue(latch1.getCount() <= 0);

    sub1.unsubscribe();
    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_call_quotes_snapshot() throws InterruptedException {
    int batchSize = 1_000_000;
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    ServiceCall service = gateway.dispatcher().create();

    CountDownLatch latch1 = new CountDownLatch(batchSize);
    Subscription sub1 = service.listen(ServiceCall.request(QuoteService.NAME, "snapshoot")
        .data(batchSize)
        .build())

        .subscribeOn(Schedulers.from(Executors.newWorkStealingPool()))
        .serialize()
        .subscribe(onNext -> latch1.countDown());
    
    
    long start = System.currentTimeMillis();
    latch1.await(batchSize / 50_000, TimeUnit.SECONDS);
    long end = (System.currentTimeMillis() - start);

    System.out.println("TIME IS UP! - recived batch (size: "
        + (batchSize - latch1.getCount()) + "/" + batchSize + ") in "
        + (System.currentTimeMillis() - start) + "ms: "
        + "rate of :" + batchSize / (end / 1000) + " events/sec ");

    sub1.unsubscribe();
    gateway.shutdown();
    node.shutdown();
  }
}