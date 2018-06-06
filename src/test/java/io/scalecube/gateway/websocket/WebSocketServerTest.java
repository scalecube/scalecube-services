package io.scalecube.gateway.websocket;

import static io.scalecube.gateway.websocket.GreetingService.GREETING_DTO_MANY;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_DTO_ONE;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_FAILING_MANY;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_FAILING_ONE;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_MANY;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_ONE;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Rule;
import org.junit.Test;

import java.time.Duration;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WebSocketServerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Rule
  public WebSocketResource resource = new WebSocketResource();

  @Test
  public void testGreetingOne() {
    resource.startServer().startServices();

    String expectedData = "Echo:hello";

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), String.class, TIMEOUT))
        .expectNextMatches(msg -> expectedData.equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingFailingOne() {
    resource.startServer().startServices();

    ServiceMessage expected = errorServiceMessage(500, "hello");

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_FAILING_ONE), ErrorData.class, TIMEOUT))
        .expectNextMatches(msg -> expected.qualifier().equals(msg.qualifier()) && expected.data().equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingMany() {
    resource.startServer().startServices();

    int n = 10;
    List<String> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .collect(Collectors.toList());

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_MANY), String.class, TIMEOUT)
        .take(n)
        .map(message -> (String) message.data()))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingFailingMany() {
    resource.startServer().startServices();

    String content = "Echo:hello";
    ServiceMessage expected = errorServiceMessage(400, content);

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_FAILING_MANY), ErrorData.class, TIMEOUT))
        .expectNextMatches(msg -> content.equals(msg.data()))
        .expectNextMatches(msg -> content.equals(msg.data()))
        .expectNextMatches(msg -> expected.qualifier().equals(msg.qualifier()) &&
            expected.data().equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedYet() {
    resource.startServer();

    ServiceMessage expected = unreachableServiceMessage(GREETING_ONE.qualifier());

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), ErrorData.class, TIMEOUT))
        .expectNextMatches(msg -> expected.qualifier().equals(msg.qualifier()) &&
            expected.data().equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesRestarted() {
    resource.startServer();

    ServiceMessage unreachableServiceMessage = unreachableServiceMessage(GREETING_ONE.qualifier());

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), ErrorData.class, TIMEOUT))
        .expectNextMatches(msg -> unreachableServiceMessage.qualifier().equals(msg.qualifier()) &&
            unreachableServiceMessage.data().equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);

    // start services node
    resource.startServices();

    String expectedData = "Echo:hello";

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), String.class, TIMEOUT))
        .expectNextMatches(msg -> expectedData.equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingDtoOne() {
    resource.startServer().startServices();

    ServiceMessage expected = serviceMessage(GREETING_DTO_ONE.qualifier(), new GreetingResponse("Echo:hello"));

    resource.sendThenReceive(Mono.just(GREETING_DTO_ONE), GreetingResponse.class, TIMEOUT)
        .subscribe(System.err::println, Throwable::printStackTrace, () -> System.err.println("FIN"));

    StepVerifier.create(Mono.defer(() -> {
      Map<String, Object> content = new HashMap<>();
      content.put("text", "Echo:hello");
      return Mono.just(ServiceMessage.builder()
          .data(content)
          .build());
    }))
        .expectNextMatches(msg -> expected.data().equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_DTO_ONE), GreetingResponse.class, TIMEOUT))
        .expectNextMatches(msg -> expected.data().equals(msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingDtoMany() {
    resource.startServer().startServices();

    int n = 10;
    List<?> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .map(GreetingResponse::new)
        .map(resp -> serviceMessage(GREETING_DTO_MANY.qualifier(), resp))
        .map(ServiceMessage::data)
        .collect(Collectors.toList());

    StepVerifier.create(Flux.fromIterable(expected)
        .map(content -> ServiceMessage.builder()
            .qualifier(GREETING_DTO_MANY.qualifier())
            .data(content)
            .build())
        .take(n)
        .map(ServiceMessage::data))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_DTO_MANY), GreetingResponse.class, TIMEOUT)
        .take(n)
        .map(ServiceMessage::data))
        .expectNextSequence(expected)
        .expectComplete()
        .verify(TIMEOUT);
  }

  private ServiceMessage unreachableServiceMessage(String qualifier) {
    int errorCode = 503;
    String errorMessage = "No reachable member with such service: " + qualifier;
    return errorServiceMessage(errorCode, errorMessage);
  }

  private ServiceMessage errorServiceMessage(int errorCode, String errorMessage) {
    return ServiceMessage.builder()
        .qualifier(Qualifier.asError(errorCode))
        .data(new ErrorData(errorCode, errorMessage))
        .build();
  }

  private ServiceMessage serviceMessage(String qualifier, GreetingResponse response) {
    Map<String, Object> content = new HashMap<>();
    content.put("text", response.getText());
    return ServiceMessage.builder()
        .qualifier(qualifier)
        .data(content)
        .build();
  }
}
