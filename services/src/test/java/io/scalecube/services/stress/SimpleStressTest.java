package io.scalecube.services.stress;

import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.services.Messages;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceCall;
import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;

import com.codahale.metrics.ConsoleReporter;
import com.codahale.metrics.MetricRegistry;

import org.junit.Test;

import java.time.Duration;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class SimpleStressTest extends BaseTest {

  int count = 600_000;

  private static AtomicInteger port = new AtomicInteger(4000);

  MetricRegistry registry = new MetricRegistry();
  ConsoleReporter reporter = ConsoleReporter.forRegistry(registry)
      .convertRatesTo(TimeUnit.SECONDS)
      .convertDurationsTo(TimeUnit.MILLISECONDS)
      .build();

  @Test
  public void test_naive_dispatcher_stress() throws InterruptedException, ExecutionException {

    // Create microservices cluster member.
    Microservices provider = Microservices.builder()
        .port(port.incrementAndGet())
        .services(new GreetingServiceImpl())
        .metrics(registry)
        .build();

    // Create microservices cluster member.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .metrics(registry)
        .build();

    reporter.start(10, TimeUnit.SECONDS);

    ServiceCall greetings = consumer.dispatcher()
        .timeout(Duration.ofSeconds(30))
        .create();

    // Measure
    CountDownLatch countLatch = new CountDownLatch(count);
    long startTime = System.currentTimeMillis();
    for (int i = 0; i < count; i++) {
      CompletableFuture<Message> future = greetings.invoke(Messages.builder()
          .request(GreetingService.class, "greetingMessage")
          .data("1")
          .build());

      future.whenComplete((success, error) -> {
        countLatch.countDown();

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
        .metrics(registry)
        .build();

    // Create microservices cluster member.
    Microservices consumer = Microservices.builder()
        .port(port.incrementAndGet())
        .seeds(provider.cluster().address())
        .metrics(registry)
        .build();
    reporter.start(10, TimeUnit.SECONDS);

    // Get a proxy to the service api.
    GreetingService service = consumer.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .timeout(Duration.ofSeconds(30))
        .create();

    // Measure
    CountDownLatch countLatch = new CountDownLatch(count);
    long startTime = System.currentTimeMillis();

    for (int i = 0; i < count; i++) {
      CompletableFuture<Message> future = service.greetingMessage(Message.fromData("naive_stress_test"));
      future.whenComplete((success, error) -> {
        if (error == null) {
          countLatch.countDown();
        } else {
          System.out.println("failed: " + error);
          fail("test_naive_stress_not_breaking_the_system failed: " + error);
        }
      });
    }

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

  }

}
