package io.scalecube.services;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import io.scalecube.transport.Message;

import org.junit.Test;

import java.lang.reflect.Field;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

public class DispatchingFutureTest {


  @Test
  public void test_dispatching_future() throws Exception {

    Microservices member = Microservices.builder().build();
    ServiceResponse response = new ServiceResponse(fn -> {
      return null;
    });

    Message request = Message.builder().correlationId(response.correlationId())
        .header(ServiceHeaders.SERVICE_RESPONSE, "").build();

    Field field = Message.class.getDeclaredField("sender");
    field.setAccessible(true);
    field.set(request, member.cluster().address());

    DispatchingFuture dispatcher = DispatchingFuture.from(member.cluster(), request);
    dispatcher.complete(new Throwable());

    CountDownLatch latch = new CountDownLatch(1);
    response.future().whenComplete((result, error) -> {
      assertTrue(error!=null);
      latch.countDown();
    });

    response.complete(
        Message.builder().header("exception", "true").data(new Exception()).build());
    
    latch.await(1, TimeUnit.SECONDS);
    member.cluster().shutdown();

  }

  @Test
  public void test_dispatching_future_error() throws Exception {

    Microservices member = Microservices.builder().build();
    ServiceResponse response = new ServiceResponse(fn -> {
      return null;
    });

    Message request = Message.builder().correlationId(response.correlationId())
        .header(ServiceHeaders.SERVICE_RESPONSE, "").build();

    Field field = Message.class.getDeclaredField("sender");
    field.setAccessible(true);
    field.set(request, member.cluster().address());

    DispatchingFuture dispatcher = DispatchingFuture.from(member.cluster(), request);
    
    CountDownLatch latch = new CountDownLatch(1);
    response.future().whenComplete((result, error) -> {
      assertTrue(error!=null);
      assertEquals(error.getMessage(),"hello");
      latch.countDown();
    });

    response.complete(
        Message.builder().header("exception", "true").data(new Exception("hello")).build());
    
    latch.await(1, TimeUnit.SECONDS);
    member.cluster().shutdown();

  }
  
  
  @Test
  public void test_dispatching_future_completeExceptionally() throws Exception {

    Microservices member = Microservices.builder().build();
    ServiceResponse response = new ServiceResponse(fn -> {
      return null;
    });

    Message request = Message.builder().correlationId(response.correlationId())
        .header(ServiceHeaders.SERVICE_RESPONSE, "").build();

    Field field = Message.class.getDeclaredField("sender");
    field.setAccessible(true);
    field.set(request, member.cluster().address());

    DispatchingFuture dispatcher = DispatchingFuture.from(member.cluster(), request);
    
    CountDownLatch latch = new CountDownLatch(1);
    response.future().whenComplete((result, error) -> {
      assertTrue(error!=null);
      assertEquals(error.getMessage(),"hello");
      latch.countDown();
    });

    response.completeExceptionally(new Exception("hello"));
    
    latch.await(1, TimeUnit.SECONDS);
    member.cluster().shutdown();

  }

}
