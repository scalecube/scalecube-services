package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.testlib.BaseTest;
import io.scalecube.transport.Message;
import io.scalecube.transport.Transport;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TransportServiceCommunicatorTest extends BaseTest {

  @Test
  public void test_transport_sender() throws InterruptedException {

    Transport transport = Transport.bindAwait();
    TransportServiceCommunicator sender = new TransportServiceCommunicator(transport);

    assertEquals(transport.address(), sender.address());

    CountDownLatch latch = new CountDownLatch(1);
    sender.listen().subscribe(onNext -> {
      latch.countDown();
    });

    sender.send(transport.address(), Message.builder().data("ping").build());
    latch.await(1, TimeUnit.SECONDS);
    assertTrue(latch.getCount() == 0);
    

  }

  @Test
  public void test_transport_sender_errors() throws InterruptedException {

    try {
      new TransportServiceCommunicator(null);
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
    assertTrue(latch.getCount() == 0);
    micro.shutdown();
  }
}
