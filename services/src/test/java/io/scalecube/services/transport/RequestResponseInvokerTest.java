package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.ServiceMessageCodec;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;
import org.reactivestreams.Publisher;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;

public class RequestResponseInvokerTest extends BaseTest {

  @Test
  public void test_request_response_invoker() throws InterruptedException {
    GreetingServiceImpl service = new GreetingServiceImpl();

    Method method = Arrays.asList(
        GreetingServiceImpl.class.getMethods())
        .stream()
        .filter(m -> m.getName().equals("sayHello"))
        .findFirst()
        .get();

    RequestResponseInvoker invoker =
        new RequestResponseInvoker(service, method, new DummyStringCodec());

    ServiceMessage message = Mono.from(invoker.invoke(ServiceMessage.builder().data("ronen").build())).block();
    System.out.println(message);

  }

 

  @Test
  public void test_invoker_registry() throws InterruptedException {


    GreetingServiceImpl service = new GreetingServiceImpl();

    LocalServiceInvoker localService = LocalServiceInvoker.create(Arrays.asList(new DummyStringCodec()),service);

    Publisher<ServiceMessage> resp =
        localService.requestResponse(ServiceMessage.builder().qualifier("sayHello").data("ronen").build());
    
    ServiceMessage message = Mono.from(resp).block();
    System.out.println(message);

  }

  @Test
  public void test_request_stream_invoker() throws InterruptedException {
    GreetingServiceImpl service = new GreetingServiceImpl();

    Method method = Arrays.asList(
        GreetingServiceImpl.class.getMethods()).stream()
        .filter(m -> m.getName().equals("greetingChannel"))
        .findFirst().get();

    RequestChannelInvoker invoker =
        new RequestChannelInvoker(service, method, new DummyStringCodec());

    Flux<ServiceMessage> stream = Flux.fromArray(new ServiceMessage[] {ServiceMessage.builder().data("ronen").build()});
    Flux<ServiceMessage> messages = Flux.from(invoker.invoke(stream));
    messages.subscribe(actual -> {
      System.out.println(actual);
    });
  }
}
