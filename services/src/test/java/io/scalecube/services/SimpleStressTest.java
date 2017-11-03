package io.scalecube.services;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.metrics.api.MetricFactory;
import io.scalecube.metrics.codahale.CodahaleMetricsFactory;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleStressTest extends BaseTest {

  private static AtomicInteger port = new AtomicInteger(4000);

  MetricRegistry registry = new MetricRegistry();
  ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build();
  MetricFactory metrics = new CodahaleMetricsFactory(registry);
  
  @Test
  public void test_naive_dispatcher_stress() throws InterruptedException, ExecutionException {
    // Create microservices cluster member.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster member.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();

    reporter.start(1, TimeUnit.SECONDS);
    
    ServiceCall service = consumer.dispatcher().metrics(metrics).create();



    // Init params
    int warmUpCount = 1_000;
    int count = 3_00_000;
    CountDownLatch warmUpLatch = new CountDownLatch(warmUpCount);

    // Warm up
    for (int i = 0; i < warmUpCount; i++) {
      // call the service.
      CompletableFuture<Message> future = service.invoke(Messages.builder()
          .request(GreetingService.class, "greetingMessage")
          .data(Message.builder().data("naive_stress_test").build())
          .build());

      future.whenComplete((success, error) -> {
        if (error == null) {
          warmUpLatch.countDown();
        }
      });
    }
    warmUpLatch.await(30, TimeUnit.SECONDS);
    assertTrue(warmUpLatch.getCount() == 0);


    // Measure
    CountDownLatch countLatch = new CountDownLatch(count);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      CompletableFuture<Message> future = service.invoke(Messages.builder()
          .request(GreetingService.class, "greetingMessage")
          .data(Message.builder().data("naive_stress_test").build())
          .build());

      future.whenComplete((success, error) -> {
        if (error == null) {
          countLatch.countDown();
        }
      });
    }
    System.out.println("Finished sending " + count + " messages in " + (System.currentTimeMillis() - startTime));
    countLatch.await(60, TimeUnit.SECONDS);
    reporter.stop();
    System.out.println("Finished receiving " + count + " messages in " + (System.currentTimeMillis() - startTime));
    assertTrue(countLatch.getCount() == 0);
    provider.shutdown().get();
    consumer.shutdown().get();
  }

  
  @Test
  public void test_naive_proxy_stress() throws InterruptedException, ExecutionException {
    // Create microservices cluster member.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .build();

    // Create microservices cluster member.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .build();
    reporter.start(1, TimeUnit.SECONDS);
    
    // Get a proxy to the service api.
    GreetingService service = createProxy(consumer);

    // Init params
    int warmUpCount = 10_000;
    int count = 300_000;
    CountDownLatch warmUpLatch = new CountDownLatch(warmUpCount);

    // Warm up
    System.out.println("starting warm-up.");
    for (int i = 0; i < warmUpCount; i++) {
      CompletableFuture<Message> future = service.greetingMessage(Message.fromData("naive_stress_test"));
      future.whenComplete((success, error) -> {
        if (error == null) {
          warmUpLatch.countDown();
        }
      });
    }
    warmUpLatch.await(4, TimeUnit.SECONDS);
    
    assertTrue(warmUpLatch.getCount() == 0);
    System.out.println("finished warm-up.");


    // Measure
    CountDownLatch countLatch = new CountDownLatch(count);
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < count; i++) {
      Util.sleep(i, 5, 300);
      CompletableFuture<Message> future = service.greetingMessage(Message.fromData("naive_stress_test"));
      future.whenComplete((success, error) -> {
        if (error == null) {
          countLatch.countDown();
        } else {
          System.out.println("failed: " + error);
          fail("test_naive_stress_not_breaking_the_system failed: " + error);
          Util.drainLatch(count, countLatch);
        }
      });
    }

    ScheduledFuture<?> sched = Executors.newScheduledThreadPool(1).scheduleAtFixedRate(() -> {
      System.out.print(countLatch.getCount() + ", ");
    }, 1, 1, TimeUnit.SECONDS);

    System.out.println("Finished sending " + count + " messages in " + (System.currentTimeMillis() - startTime));
    countLatch.await(60, TimeUnit.SECONDS);

    System.out.println("Finished receiving " + (count - countLatch.getCount()) + " messages in "
        + (System.currentTimeMillis() - startTime));

    System.out.println("Rate: " + ((count - countLatch.getCount()) / ((System.currentTimeMillis() - startTime) / 1000))
        + " round-trips/sec");

    reporter.stop();
    assertTrue(countLatch.getCount() == 0);

    provider.shutdown().get();
    consumer.shutdown().get();
    sched.cancel(true);
  }
  
  private GreetingService createProxy(Microservices gateway) {
    return gateway.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .timeout(Duration.ofSeconds(30))
        .metrics(metrics)
        .create();
  }
}
