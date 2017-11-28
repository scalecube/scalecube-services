package io.scalecube.services.streaming;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.Messages;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import com.codahale.metrics.MetricRegistry;

import org.junit.Test;

import rx.Observable;
import rx.Subscription;
import rx.schedulers.Schedulers;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;

public class TestStreamingService extends BaseTest {

  MetricRegistry registry = new MetricRegistry();
  

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
    assertTrue(latch.getCount() == 0);
  }

  @Test
  public void test_local_quotes_service() throws InterruptedException {
    Microservices node = Microservices.builder().services(new SimpleQuoteService()).build();

    QuoteService service = node.proxy().api(QuoteService.class).create();

    CountDownLatch latch = new CountDownLatch(3);
    Observable<String> obs = service.quotes(2);

    Subscription sub = obs.subscribe(onNext -> {
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
    assertTrue(latch1.getCount() == 0);
    assertTrue(latch2.getCount() == 0);
    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_quotes_snapshot() throws InterruptedException {
    int batchSize = 500_000;
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService())
        .metrics(registry)
        .build();

    QuoteService service = gateway.proxy().api(QuoteService.class).create();

    CountDownLatch latch1 = new CountDownLatch(batchSize);

    Subscription sub1 = service.snapshoot(batchSize)
        .subscribeOn(Schedulers.from(Executors.newCachedThreadPool()))
        .serialize()
        .subscribe(onNext -> latch1.countDown());

    long start = System.currentTimeMillis();
    latch1.await(batchSize / 20_000, TimeUnit.SECONDS);
    long end = (System.currentTimeMillis() - start);

    System.out.println("TIME IS UP! - recived batch (size: "
        + (batchSize - latch1.getCount()) + "/" + batchSize + ") in "
        + (System.currentTimeMillis() - start) + "ms: "
        + "rate of :" + batchSize / (end / 1000) + " events/sec ");
    System.out.println(registry.getMeters());
    assertTrue(latch1.getCount() == 0);
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
    Subscription sub1 = service.listen(Messages.builder()
        .request(QuoteService.NAME, "snapshoot")
        .data(batchSize)
        .build())

        .subscribeOn(Schedulers.from(Executors.newCachedThreadPool()))
        .serialize()
        .subscribe(onNext -> latch1.countDown());


    long start = System.currentTimeMillis();
    latch1.await(batchSize / 40_000, TimeUnit.SECONDS);
    long end = (System.currentTimeMillis() - start);

    System.out.println("TIME IS UP! - recived batch (size: "
        + (batchSize - latch1.getCount()) + "/" + batchSize + ") in "
        + (System.currentTimeMillis() - start) + "ms: "
        + "rate of :" + batchSize / (end / 1000) + " events/sec ");

    sub1.unsubscribe();
    gateway.shutdown();
    node.shutdown();
  }

  @Test
  public void test_just_once() throws InterruptedException {
    int batchSize = 1;
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    QuoteService service = gateway.proxy().api(QuoteService.class).create();

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Subscription> sub1 = new AtomicReference<Subscription>(null);
    sub1.set(service.justOne()
        .serialize().subscribe(onNext -> {
          sub1.get().unsubscribe();
          latch1.countDown();
        }));

    latch1.await(2, TimeUnit.SECONDS);
    assertTrue(latch1.getCount() == 0);
    assertTrue(sub1.get().isUnsubscribed());

    gateway.shutdown();
    node.shutdown();

  }

  @Test
  public void test_just_one_message() throws InterruptedException {
    int batchSize = 1;
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    ServiceCall service = gateway.dispatcher().create();

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Subscription> sub1 = new AtomicReference<Subscription>(null);
    Message justOne = Messages.builder().request(QuoteService.NAME, "justOne").build();

    sub1.set(service.listen(justOne)
        .serialize().subscribe(onNext -> {
          sub1.get().unsubscribe();
          latch1.countDown();
        }));

    latch1.await(2, TimeUnit.SECONDS);
    assertTrue(latch1.getCount() == 0);
    assertTrue(sub1.get().isUnsubscribed());
    gateway.shutdown();
    node.shutdown();

  }

  @Test
  public void test_scheduled_messages() throws InterruptedException {
    int batchSize = 1;
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    ServiceCall service = gateway.dispatcher().create();

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Subscription> sub1 = new AtomicReference<Subscription>(null);
    Message scheduled = Messages.builder().request(QuoteService.NAME, "scheduled")
        .data(1000).build();

    sub1.set(service.listen(scheduled)
        .serialize().subscribe(onNext -> {
          sub1.get().isUnsubscribed();
          latch1.countDown();

        }));

    latch1.await(2, TimeUnit.SECONDS);
    assertTrue(latch1.getCount() == 0);
    node.shutdown();
    gateway.shutdown();
  }

  @Test
  public void test_unknown_method() throws InterruptedException {

    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    ServiceCall service = gateway.dispatcher().create();

    final CountDownLatch latch1 = new CountDownLatch(1);

    Message scheduled = Messages.builder().request(QuoteService.NAME, "unknonwn").build();
    try {
      service.listen(scheduled);
    } catch (Exception ex) {
      if (ex.getMessage().contains("No reachable member with such service: unknonwn")) {
        latch1.countDown();
      }
    }

    latch1.await(2, TimeUnit.SECONDS);
    assertTrue(latch1.getCount() == 0);
    node.shutdown();
    gateway.shutdown();

  }

  @Test
  public void test_remote_node_died() throws InterruptedException {
    int batchSize = 1;
    Microservices gateway = Microservices.builder().build();

    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services(new SimpleQuoteService()).build();

    ServiceCall service = gateway.dispatcher().create();

    final CountDownLatch latch1 = new CountDownLatch(batchSize);
    AtomicReference<Subscription> sub1 = new AtomicReference<Subscription>(null);
    Message justOne = Messages.builder().request(QuoteService.NAME, "justOne").build();

    sub1.set(service.listen(justOne)
        .subscribe(onNext -> {
          System.out.println(onNext);
        }));

    gateway.cluster().listenMembership()
        .filter(predicate -> predicate.isRemoved())
        .subscribe(onNext -> {
          latch1.countDown();
        });

    node.cluster().shutdown();

    latch1.await(20, TimeUnit.SECONDS);
    Thread.sleep(100);
    assertTrue(latch1.getCount() == 0);
    assertTrue(sub1.get().isUnsubscribed());
    gateway.shutdown();


  }
}
