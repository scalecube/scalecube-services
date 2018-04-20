package io.scalecube.services.transport;

import io.scalecube.services.Reflect;
import io.scalecube.services.api.ServiceMessage;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;
import org.reactivestreams.Publisher;

import java.lang.reflect.Method;
import java.util.Arrays;
import java.util.Map;

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
    ServicesMessageAcceptorRegistry registry = new ServicesMessageAcceptorRegistry();

    GreetingServiceImpl service = new GreetingServiceImpl();

    Reflect.serviceInterfaces(service).forEach(serviceIinterface -> {
      Map<String, Method> methods = Reflect.serviceMethods(serviceIinterface);
      methods.entrySet().forEach(entry -> {

        switch (Reflect.exchangeTypeOf(entry.getValue())) {
          case REQUEST_RESPONSE:
            registry.register(entry.getKey(),
                new RequestResponseInvoker(service, entry.getValue(), new DummyStringCodec()));
            break;

          case REQUEST_CHANNEL:
            registry.register(entry.getKey(),
                new RequestChannelInvoker(service, entry.getValue(), new DummyStringCodec()));
            break;
        }
      });
    });

    Publisher<ServiceMessage> resp =
        registry.requestResponse(ServiceMessage.builder().qualifier("sayHello").data("ronen").build());
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
