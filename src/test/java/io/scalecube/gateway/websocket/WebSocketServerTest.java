package io.scalecube.gateway.websocket;

import static org.junit.Assert.assertEquals;

import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Mono;

import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.stream.IntStream;

public class WebSocketServerTest {

  public static final Duration TIMEOUT = Duration.ofSeconds(3);

  public static final ServiceMessage greetingOne =
      ServiceMessage.builder().qualifier("/greeting/one").data("hello").build();

  public static final ServiceMessage greetingMany =
      ServiceMessage.builder().qualifier("/greeting/many").data("hello").build();

  @Rule
  public WebSocketResource resource = new WebSocketResource();

  @Test
  public void testGreetingOne() {
    resource.startServer().startServices();

    String echo =
        resource.sendThenReceive(Mono.just(greetingOne), TIMEOUT)
            .map(message -> (String) message.data())
            .blockFirst(TIMEOUT);

    assertEquals("Echo:hello", echo);
  }

  @Test
  public void testGreetingMany() {
    resource.startServer().startServices();

    int n = 10;
    List<String> echoMessages =
        resource.sendThenReceive(Mono.just(greetingMany), TIMEOUT)
            .take(n)
            .map(message -> (String) message.data())
            .collectList().block(TIMEOUT);

    assertEquals(n, echoMessages.size());
    IntStream.range(0, n).forEach(i -> assertEquals("Greeting (" + i + ") to: hello", echoMessages.get(i)));
  }

  @Test
  public void testServicesNotStartedYet() {
    resource.startServer();

    ServiceMessage error =
        resource.sendThenReceive(Mono.just(greetingOne), TIMEOUT).blockFirst(TIMEOUT);

    assertEquals("Echo:hello", error);
  }

  @Test
  public void testServicesRestarted() {
    resource.startServer();

    ServiceMessage error =
        resource.sendThenReceive(Mono.just(greetingOne), TIMEOUT).blockFirst(TIMEOUT);

    assertEquals("Echo:hello", error);

    // start services node
    resource.startServices();

    ServiceMessage echo =
        resource.sendThenReceive(Mono.just(greetingOne), TIMEOUT).blockFirst(TIMEOUT);

    assertEquals("Echo:hello", echo.data());
  }
}
