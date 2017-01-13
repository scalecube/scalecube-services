package io.scalecube.hyper.scale;

import io.scalecube.services.GreetingService;
import io.scalecube.services.GreetingServiceImpl;
import io.scalecube.services.Microservices;
import io.scalecube.transport.Address;
import io.scalecube.transport.Message;

import org.junit.Test;

import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.Executors;

public class HyperScaleTest {

  private boolean done;
  private ConcurrentMap<Address, Address> responses = new ConcurrentHashMap<>();

  @Test
  public void test_hyper_scale() throws InterruptedException {
    Microservices seed = Microservices.builder().build();

    Executors.newSingleThreadExecutor().submit(()->{
      execute(seed);
    });

    for (int i = 0; i < 230; i = i + 2) {
      Microservices.builder().services(new GreetingServiceImpl()).port(4803 + i).seeds(seed.cluster().address())
          .build();
      System.out.println( responses);
      System.out.println("cluster size: " + seed.cluster().members().size());
      System.out.println("responses size: " + responses.size());
    }
    done = true;

  }

  private void execute(Microservices seed ) {
    GreetingService proxy = seed.proxy().api(GreetingService.class).create();
    Message request = Message.builder().data("hello").build();
    sleep(2000);
    while (!done) {
      proxy.greetingMessage(request).whenComplete((action, err) -> {
        responses.put(action.sender(),action.sender());
      });
      sleep(10);
    }

  }

  private void sleep(int time) {
    try {
      Thread.sleep(time);
    } catch (InterruptedException e) {
      // TODO Auto-generated catch block
      e.printStackTrace();
    }
  }
}
