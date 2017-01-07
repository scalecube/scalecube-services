package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.assertNotEquals;

import io.scalecube.transport.Transport;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransportSenderTest {

  @Test
  public void test_transport_sender() throws InterruptedException {

    Transport transport = Transport.bindAwait();
    TransportSender sender = new TransportSender(transport);

    assertEquals(transport.address(), sender.address());

    CountDownLatch latch = new CountDownLatch(1);
    sender.listen().subscribe(onNext -> {
      latch.countDown();
    });

    latch.await(1, TimeUnit.SECONDS);
  }

  @Test
  public void test_transport_sender_errors() throws InterruptedException {

    try {
      new TransportSender(null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: transport can't be null");
    }
  }
  
  @Test
  public void test_use_transport_sender() throws InterruptedException {
    Microservices micro = Microservices.builder()
        .services(new GreetingServiceImpl())
        .build();

    GreetingService service = micro.proxy().api(GreetingService.class).create();

    CountDownLatch latch = new CountDownLatch(1);
    service.greeting("joe").whenComplete((result, error) -> {
      if (error == null) {
        latch.countDown();
      }
    });

    assertTrue(!micro.services().isEmpty());
    ServiceInstance instance = micro.services().stream().findFirst().get();
    assertNotEquals(instance.address(), micro.cluster().address());
    assertEquals(instance.serviceName(), "io.scalecube.services.GreetingService");
    latch.await(1, TimeUnit.SECONDS);

    micro.shutdown();
  }
}
