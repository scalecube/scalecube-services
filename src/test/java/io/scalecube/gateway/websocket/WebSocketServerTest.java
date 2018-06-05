package io.scalecube.gateway.websocket;

import static org.junit.Assert.assertEquals;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.List;
import java.util.Objects;
import java.util.stream.Collectors;
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
    List<String> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .collect(Collectors.toList());

    StepVerifier.create(resource.sendThenReceive(Mono.just(greetingMany), TIMEOUT)
            .take(n)
        .map(message -> (String) message.data()))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedYet() {
    resource.startServer();

    int expectedErrorCode = 503;
    String expectedQualifier = Qualifier.asError(expectedErrorCode);
    String expectedErrorMessage = "No reachable member with such service: " + greetingOne.qualifier();

    StepVerifier.create(resource.sendThenReceive(Mono.just(greetingOne), TIMEOUT))
        .expectNextMatches(msg -> {
          ErrorData data = msg.data();
          return Objects.equals(expectedQualifier, msg.qualifier()) &&
              Objects.equals(expectedErrorCode, data.getErrorCode()) &&
              Objects.equals(expectedErrorMessage, data.getErrorMessage());
        })
        .expectComplete()
        .verify(TIMEOUT);
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
