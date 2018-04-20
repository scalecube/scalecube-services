package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RequestResponseInvokerTest extends BaseTest {

  @Test
  public void test_request_response_invoker() throws InterruptedException {
    GreetingService service = new GreetingService();

    Method method = Arrays.asList(
        GreetingService.class.getMethods())
        .stream()
        .filter(m -> m.getName().equals("sayHello"))
        .findFirst()
        .get();

    RequestResponseInvoker invoker =
        new RequestResponseInvoker(service, method, String.class, String.class, new DummyStringCodec());

    ServiceMessage message = Mono.from(invoker.invoke(ServiceMessage.builder().data("ronen").build())).block();
    System.out.println(message);

  }

  @Test
  public void test_request_stream_invoker() throws InterruptedException {
    GreetingService service = new GreetingService();

    Method method = Arrays.asList(
        GreetingService.class.getMethods())
        .stream()
        .filter(m -> m.getName().equals("greetingChannel"))
        .findFirst()
        .get();

    RequestChannelInvoker invoker =
        new RequestChannelInvoker(service, method, String.class, String.class, new DummyStringCodec());
    Flux<ServiceMessage> stream = Flux.fromArray(new ServiceMessage[] {ServiceMessage.builder().data("ronen").build()});
    Flux<ServiceMessage> messages = Flux.from(invoker.invoke(stream));
    messages.subscribe(actual -> {
      System.out.println(actual);
    });


  }
}
