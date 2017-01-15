package io.scalecube.services.inject;

import static org.junit.Assert.assertTrue;

import io.scalecube.services.CoarseGrainedConfigurableServiceImpl;
import io.scalecube.services.CoarseGrainedService;
import io.scalecube.services.CoarseGrainedServiceImpl;
import io.scalecube.services.GreetingServiceImpl;
import io.scalecube.services.Microservices;
import io.scalecube.services.ServiceInjector;

import org.junit.Test;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

public class ServiceInjectorsTest {

  private static AtomicInteger port = new AtomicInteger(4000);

  @Test
  public void test_serviceA_calls_serviceB_with_injector() throws InterruptedException {

    Microservices gateway = createSeed();

    // Create microservices instance cluster.
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services().from(CoarseGrainedServiceImpl.class, GreetingServiceImpl.class).build() // add service a and b
                                                                                            // classes
        .build();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.proxy().api(CoarseGrainedService.class).create();
    CountDownLatch countLatch = new CountDownLatch(1);
    CompletableFuture<String> future = service.callGreeting("joe");
    future.whenComplete((success, error) -> {
      if (error == null) {
        assertTrue(success.equals(" hello to: joe"));
        countLatch.countDown();
      }
    });

    countLatch.await(5, TimeUnit.SECONDS);
    assertTrue(countLatch.getCount() == 0);
    gateway.cluster().shutdown();
    provider.cluster().shutdown();

  }

  @Test
  public void test_serviceA_calls_serviceB_with_external_injector() throws InterruptedException {

    Microservices gateway = createSeed();

    ServiceInjector injector = ServiceInjector.builder().bind(CoarseGrainedConfigurableServiceImpl.Configuration.class)
        .to(new CoarseGrainedConfigurableServiceImpl.Configuration("English")).build();
    // Create microservices instance cluster.
    Microservices provider = Microservices.builder()
        .seeds(gateway.cluster().address())
        .port(port.incrementAndGet())
        .services().from(CoarseGrainedConfigurableServiceImpl.class, GreetingServiceImpl.class).build() // add service a
                                                                                                        // and b classes
        .injector(injector)
        .build();

    // Get a proxy to the service api.
    CoarseGrainedService service = gateway.proxy().api(CoarseGrainedService.class).create();
    CountDownLatch countLatch = new CountDownLatch(1);
    CompletableFuture<String> future = service.callGreeting("joe");
    future.whenComplete((success, error) -> {
      if (error == null) {
        assertTrue(success.equals(" hello to: joe"));
        countLatch.countDown();
      }
    });

    countLatch.await(5, TimeUnit.SECONDS);
    assertTrue(countLatch.getCount() == 0);
    gateway.shutdown();
    provider.shutdown();

  }

  @Test
  public void test_serviceA_calls_serviceB_with_external_injector_error() throws InterruptedException {

    Microservices gateway = createSeed();

    CountDownLatch countLatch = new CountDownLatch(1);
    // Create microservices instance cluster.
    try {
      Microservices provider = Microservices.builder()
          .seeds(gateway.cluster().address())
          .port(port.incrementAndGet())
          .services().from(CoarseGrainedConfigurableServiceImpl.class, GreetingServiceImpl.class).build() // add service
                                                                                                          // a and b
                                                                                                          // classes
          .build();
    } catch (Exception ex) {
      assertTrue(ex.getMessage().equals("java.lang.IllegalArgumentException: wrong number of arguments"));
      countLatch.countDown();
    }
    assertTrue(countLatch.getCount() == 0);
    gateway.cluster().shutdown();

  }

  private Microservices createSeed() {
    return Microservices.builder()
        .port(port.incrementAndGet())
        .build();
  }
}
