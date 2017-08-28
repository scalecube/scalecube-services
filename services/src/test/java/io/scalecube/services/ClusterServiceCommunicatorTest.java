package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.cluster.Cluster;
import io.scalecube.transport.Message;

import org.junit.Test;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class ClusterServiceCommunicatorTest {

  @Test
  public void test_cluster_sender() throws InterruptedException {

    Cluster cluster = Cluster.joinAwait();
    ClusterServiceCommunicator sender = new ClusterServiceCommunicator(cluster);

    assertEquals(cluster.address(), sender.address());

    CountDownLatch latch = new CountDownLatch(1);
    sender.listen().subscribe(onNext -> {
      latch.countDown();
    });

    sender.send(cluster.address(), Message.builder().data("ping").build());
    latch.await(1, TimeUnit.SECONDS);
    assertTrue(latch.getCount() == 0);

    cluster.shutdown();
  }

  @Test
  public void test_cluster_sender_errors() throws InterruptedException {

    try {
      new ClusterServiceCommunicator(null);
    } catch (Exception ex) {
      assertEquals(ex.toString(), "java.lang.IllegalArgumentException: cluster can't be null");
    }
  }

  @Test
  public void test_reuse_cluster_sender() throws InterruptedException {
    Microservices micro = Microservices.builder()
        .services(new GreetingServiceImpl())
        .reuseClusterTransport(true)
        .build();

    GreetingService service = micro.proxy().api(GreetingService.class).create();

    CountDownLatch latch = new CountDownLatch(1);
    service.greeting("joe").whenComplete((result, error) -> {
      if (error == null) {
        latch.countDown();
      } else {
        System.out.println("test_reuse_cluster_sender failed:  " + error);
      }
    });

    assertTrue(!micro.services().isEmpty());
    ServiceInstance instance = micro.services().stream().findFirst().get();
    assertEquals(instance.address(), micro.cluster().address());
    assertEquals(instance.serviceName(), "io.scalecube.services.GreetingService");
    latch.await(1, TimeUnit.SECONDS);
    assertTrue(latch.getCount() == 0);
    micro.shutdown();
  }
}
