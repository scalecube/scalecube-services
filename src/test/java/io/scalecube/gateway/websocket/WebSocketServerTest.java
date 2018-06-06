package io.scalecube.gateway.websocket;

import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;
import org.junit.Rule;
import org.junit.Test;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static io.scalecube.gateway.websocket.GreetingService.GREETING_FAILING_MANY;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_FAILING_ONE;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_MANY;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_ONE;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_POJO_MANY;
import static io.scalecube.gateway.websocket.GreetingService.GREETING_POJO_ONE;
import static org.junit.Assert.assertEquals;

public class WebSocketServerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  @Rule
  public WebSocketResource resource = new WebSocketResource();

  @Test
  public void testGreetingOne() {
    resource.startServer().startServices();

    String expectedData = "Echo:hello";

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), String.class, TIMEOUT))
        .assertNext(msg -> assertEquals(expectedData, msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingFailingOne() {
    resource.startServer().startServices();

    ServiceMessage expected = errorServiceMessage(500, "hello");

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_FAILING_ONE), ErrorData.class, TIMEOUT))
        .assertNext(message -> {
          assertEquals(expected.qualifier(), message.qualifier());
          ErrorData actualData = message.data();
          ErrorData expectedData = expected.data();
          assertEquals(expectedData.getErrorCode(), actualData.getErrorCode());
          assertEquals(expectedData.getErrorMessage(), actualData.getErrorMessage());
        })
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

    List<String> actual = resource.sendThenReceive(Mono.just(GREETING_MANY), String.class, TIMEOUT)
        .take(n)
        .map(ServiceMessage::data)
        .cast(String.class)
        .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testGreetingFailingMany() {
    resource.startServer().startServices();

    String content = "Echo:hello";
    ServiceMessage expected = errorServiceMessage(500, content);

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_FAILING_MANY), ErrorData.class, TIMEOUT))
        .assertNext(msg -> assertEquals(content, msg.data()))
        .assertNext(msg -> assertEquals(content, msg.data()))
        .assertNext(msg -> {
          assertEquals(expected.qualifier(), msg.qualifier());
          ErrorData actualData = msg.data();
          ErrorData expectedData = expected.data();
          assertEquals(expectedData.getErrorCode(), actualData.getErrorCode());
          assertEquals(expectedData.getErrorMessage(), actualData.getErrorMessage());
        })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedYet() {
    resource.startServer();

    ServiceMessage expected = unreachableServiceMessage(GREETING_ONE.qualifier());

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), ErrorData.class, TIMEOUT))
        .assertNext(msg -> {
          assertEquals(expected.qualifier(), msg.qualifier());
          ErrorData actualData = msg.data();
          ErrorData expectedData = expected.data();
          assertEquals(expectedData.getErrorCode(), actualData.getErrorCode());
          assertEquals(expectedData.getErrorMessage(), actualData.getErrorMessage());
        })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesRestarted() {
    resource.startServer();

    ServiceMessage unreachableServiceMessage = unreachableServiceMessage(GREETING_ONE.qualifier());

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), ErrorData.class, TIMEOUT))
        .assertNext(msg -> {
          assertEquals(unreachableServiceMessage.qualifier(), msg.qualifier());
          ErrorData actualData = msg.data();
          ErrorData expectedData = unreachableServiceMessage.data();
          assertEquals(expectedData.getErrorCode(), actualData.getErrorCode());
          assertEquals(expectedData.getErrorMessage(), actualData.getErrorMessage());
        })
        .expectComplete()
        .verify(TIMEOUT);

    // start services node
    resource.startServices();

    String expectedData = "Echo:hello";

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_ONE), String.class, TIMEOUT))
        .assertNext(msg -> assertEquals(expectedData, msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoOne() {
    resource.startServer().startServices();

    String expectedQualifier = GREETING_POJO_ONE.qualifier();
    GreetingResponse expectedData = new GreetingResponse("Echo:hello");

    StepVerifier.create(resource.sendThenReceive(Mono.just(GREETING_POJO_ONE), GreetingResponse.class, TIMEOUT))
        .assertNext(msg -> {
          assertEquals(expectedQualifier, msg.qualifier());
          GreetingResponse actualData = msg.data();
          assertEquals(expectedData.getText(), actualData.getText());
        })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoMany() {
    resource.startServer().startServices();

    int n = 10;
    List<GreetingResponse> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .map(GreetingResponse::new)
        .collect(Collectors.toList());

    List<GreetingResponse> actual = resource.sendThenReceive(Mono.just(GREETING_POJO_MANY), GreetingResponse.class, TIMEOUT)
        .take(n)
        .map(ServiceMessage::data)
        .cast(GreetingResponse.class)
        .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
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
}
