package io.scalecube.services;

import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Message;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ServicesStressTest {

  // Init params
  private static int warmUpCount = 10_000;
  private static int count = 320_000;

  private static AtomicInteger port = new AtomicInteger(4000);

  @Test
  public void test_naive_stress_not_breaking_the_system() {
    try {
      // Create microservices cluster member.
      Microservices gateway = Microservices.builder()
          .port(port.incrementAndGet())
          .build();

      int cores = Runtime.getRuntime().availableProcessors()/2;

      // Create microservices cluster member.
      Microservices.builder()
          .seeds(gateway.cluster().address())
          .port(port.incrementAndGet())
          .services(new GreetingServiceImpl())
          .build();


      ConcurrentHashMap<Integer, GreetingService> proxies = new ConcurrentHashMap<>();
      for (int i = 0; i < cores; i++) {
        proxies.put(i, createProxy(gateway));
      }

      CountDownLatch warmUpLatch = new CountDownLatch(warmUpCount);

      // Warm up
      for (int i = 0; i < warmUpCount; i++) {
        int select = (i % cores);
        CompletableFuture<Message> future = proxies.get(select).greetingMessage(Message.fromData("naive_stress_test"));
        future.whenComplete((success, error) -> {
          if (error == null) {
            warmUpLatch.countDown();
          }
        });
      }

      warmUpLatch.await(30, TimeUnit.SECONDS);

      TimeUnit.SECONDS.sleep(1);

      assertTrue(warmUpLatch.getCount() == 0);

      // Measure
      CountDownLatch countLatch = new CountDownLatch(count);
      long startTime = System.currentTimeMillis();
      ExecutorService exec = Executors.newFixedThreadPool(cores);

      for (int x = 0; x < cores; x++) {
        exec.execute(() -> {
          for (int i = 0; i < count / cores; i++) {
            int select = (i % cores);
            CompletableFuture<Message> future =
                proxies.get(select).greetingMessage(Message.fromData("naive_stress_test"));
            future.whenComplete((success, error) -> {
              if (error == null) {
                countLatch.countDown();
              }
            });
          }
          System.out.println("Finished sending " + count / cores + " messages in "
              + (System.currentTimeMillis() - startTime));
        });
      }

      countLatch.await(160, TimeUnit.SECONDS);
      long endTime = (System.currentTimeMillis() - startTime);
      System.out.println("Finished receiving " + count + " messages in " + endTime);
      System.out.println("query per secound: " + count / endTime + "K");
      System.out.println("query per secound/core: (" + cores + ") :" + (count / endTime) / cores + "K");

      TimeUnit.SECONDS.sleep(1);
      assertTrue(countLatch.getCount() == 0);

    } catch (Exception ex) {
      System.out.println(ex);
    }

  }

  private GreetingService createProxy(Microservices gateway) {
    return gateway.proxy()
        .api(GreetingService.class) // create proxy for GreetingService API
        .create();
  }

}
