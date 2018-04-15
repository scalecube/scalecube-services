package io.scalecube.services;

import static org.hamcrest.CoreMatchers.instanceOf;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import io.scalecube.services.ServiceCall.Call;
import io.scalecube.streams.StreamMessage;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import rx.exceptions.Exceptions;

import java.util.concurrent.CompletableFuture;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * this example shows the graceful shutdown behavior. it demonstrate the behavior of a service being consumed while the
 * provider is shutting down and it leaves the cluster. service provider continue to serve messages until the cluster
 * realizes that the node has left. once cluster realizes member has left consumers stop routing messages to it. node
 * can safely shutdown completely.
 * 
 */
public class GracefulShutdownTest extends BaseTest {



  @Test()
  public void test_gracefull_shutdown() throws InterruptedException {

    // create cluster members with 3 nodes: gateway, node1, node2
    // node 1 and 2 provision GreetingService instance (each).
    Members members = Members.create();
    // get a proxy to the service api.
    Call service = members.gateway().call().responseTypeOf(GreetingResponse.class);

    // call the service.
    AtomicInteger count = new AtomicInteger(3);
    StreamMessage request = Messages.builder()
        .request(GreetingService.class, "greeting")
        .data("joe")
        .build();

    AtomicInteger postShutdown = new AtomicInteger(3);
    // continue with the test while node1 is still active in the cluster
    while (members.gateway().cluster().member(members.node1().cluster().address()).isPresent()
        || postShutdown.get() >= 0) {
      
      CompletableFuture<StreamMessage> future = service.invoke(request);
      future.whenComplete((result, ex) -> {
        if (ex == null) {
          // print the greeting.
          assertThat(result.data(), instanceOf(GreetingResponse.class));
          assertTrue(((GreetingResponse) result.data()).getResult().equals(" hello to: joe"));
          System.out.println(count.get() + " - Response from node: ");
          count.decrementAndGet();
        } else {
          fail(); // if one request fails fail the test
          // print the greeting.
          System.out.println(ex);
          Exceptions.propagate(ex);
        }
      });

      if (count.get() == 0) {
        //  node1 leave the cluster after on 0.
        members.node1().cluster().shutdown();
      }

      // sending messages after member is gone.
      // node still answer requests as its only half stopped.
      if (!members.gateway().cluster().member(members.node1().cluster().address()).isPresent()) {
        postShutdown.decrementAndGet();
      }

      sleep(1000);
    }

    members.shutdown();

  }


  final private static class Members {

    private Microservices gateway;
    private Microservices node1;
    private Microservices node2;

    public Members(Microservices gateway,
        Microservices node1,
        Microservices node2) {
      this.gateway = gateway;
      this.node1 = node1;
      this.node2 = node2;

    }

    public static Members create() {
      // Create microservices cluster.
      Microservices gateway = Microservices.builder().build();

      // Create microservices cluster.
      Microservices node1 = Microservices.builder()
          .seeds(gateway.cluster().address())
          .services(new GreetingServiceImpl())
          .build();

      Microservices node2 = Microservices.builder()
          .seeds(gateway.cluster().address())
          .services(new GreetingServiceImpl())
          .build();

      return new Members(gateway, node1, node2);

    }

    public Microservices gateway() {
      return this.gateway;
    }

    public Microservices node1() {
      return this.node1;
    }

    public Microservices node2() {
      return this.node2;
    }

    public void shutdown() {
      gateway.shutdown();
      node1.shutdown();
      node2.shutdown();
    }
  }

  private void sleep(int interval) {
    try {
      Thread.sleep(interval);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }

}
