package io.scalecube.services.transport;

import io.scalecube.services.api.ServiceMessage;
import io.scalecube.testlib.BaseTest;

import org.junit.Test;

import java.lang.reflect.Method;
import java.util.Arrays;

import reactor.core.publisher.Mono;

public class RequestResponseInvokerTest extends BaseTest {

  @Test
  public void test_quotes() throws InterruptedException {
    GreetingService service = new GreetingService();

    Method method = Arrays.asList(
        GreetingService.class.getMethods())
        .stream()
        .filter(m -> m.getName().equals("sayHello"))
        .findFirst()
        .get();
    
    RequestResponseInvoker invoker =
        new RequestResponseInvoker(service, method, String.class, String.class, new DummyStringCodec());
    
    ServiceMessage message = Mono.from( invoker.invoke(ServiceMessage.builder().data("ronen").build())).block();
    System.out.println(message);

  }
}
