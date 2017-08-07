package io.scalecube.services.streams;

import static org.junit.Assert.*;

import io.scalecube.services.Microservices;

import org.junit.Test;

import rx.Subscription;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class TestStreamingService {

  @Test
  public void test_quotes() {


    StreamingService service = new SimpleStreamingService();

    service.quotes(2).subscribe(onNext -> {
      System.out.println(onNext);
    });
  }

  @Test
  public void test_local_quotes_service() {
    Microservices node = Microservices.builder().services( new SimpleStreamingService()).build();
    
    StreamingService service = node.proxy().api(StreamingService.class).create();
    
    service.quotes(2).subscribe(onNext -> {
      System.out.println(onNext);
    });
  }
  
  @Test
  public void test_remote_quotes_service() throws InterruptedException {
    Microservices gateway = Microservices.builder().build();
    
    Microservices node = Microservices.builder()
        .seeds(gateway.cluster().address())
        .services( new SimpleStreamingService()).build();
    
    StreamingService service = gateway.proxy().api(StreamingService.class).create();
    CountDownLatch latch = new CountDownLatch(3);
    Subscription sub = service.quotes(2).subscribe(onNext -> {
      System.out.println(onNext);
      latch.countDown();
    });
    latch.await(4, TimeUnit.SECONDS);
    sub.unsubscribe();
    assertTrue(latch.getCount()==0);
    System.out.println("done");
  }
}
