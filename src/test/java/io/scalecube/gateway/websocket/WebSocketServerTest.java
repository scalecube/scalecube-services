package io.scalecube.gateway.websocket;

import static org.hamcrest.Matchers.instanceOf;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import io.scalecube.gateway.MicroservicesResource;
import io.scalecube.gateway.WebsocketResource;
import io.scalecube.gateway.examples.GreetingRequest;
import io.scalecube.gateway.examples.GreetingResponse;
import io.scalecube.services.api.ErrorData;
import io.scalecube.services.api.Qualifier;
import io.scalecube.services.api.ServiceMessage;

import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.test.StepVerifier;

import org.junit.Rule;
import org.junit.Test;
import org.reactivestreams.Publisher;

import java.time.Duration;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

public class WebSocketServerTest {

  private static final Duration TIMEOUT = Duration.ofSeconds(3);

  private static final int REQUEST_NUM = 3;

  private static final ServiceMessage GREETING_ONE =
      ServiceMessage.builder().qualifier("/greeting/one").data("hello").build();

  private static final ServiceMessage GREETING_FAILING_ONE =
      ServiceMessage.builder().qualifier("/greeting/failing/one").data("hello").build();

  private static final ServiceMessage GREETING_MANY =
      ServiceMessage.builder().qualifier("/greeting/many").data("hello").build();

  private static final ServiceMessage GREETING_FAILING_MANY =
      ServiceMessage.builder().qualifier("/greeting/failing/many").data("hello").build();

  private static final ServiceMessage GREETING_POJO_ONE =
      ServiceMessage.builder().qualifier("/greeting/pojo/one").data(new GreetingRequest("hello")).build();

  private static final ServiceMessage GREETING_POJO_MANY =
      ServiceMessage.builder().qualifier("/greeting/pojo/many").data(new GreetingRequest("hello")).build();

  @Rule
  public MicroservicesResource microservicesResource = new MicroservicesResource();

  @Rule
  public WebsocketResource websocketResource = new WebsocketResource();

  @Test
  public void testGreetingOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    Publisher<ServiceMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_ONE);

    StepVerifier.FirstStep<ServiceMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, String.class, TIMEOUT));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> {
        assertThat(msg.data(), instanceOf(String.class));
        assertEquals("Echo:hello", msg.data());
      });
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingFailingOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    ServiceMessage error = errorServiceMessage(500, "hello");

    Publisher<ServiceMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_FAILING_ONE);

    StepVerifier.FirstStep<ServiceMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, ErrorData.class, TIMEOUT));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testGreetingMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    int expectedResponseNum = 10;
    List<String> expected = IntStream.range(0, expectedResponseNum)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .collect(Collectors.toList());

    List<String> actual =
        websocketResource.sendMessages(Mono.just(GREETING_MANY), String.class, TIMEOUT)
            .take(expectedResponseNum)
            .map(ServiceMessage::data)
            .cast(String.class)
            .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testGreetingFailingMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    String content = "Echo:hello";
    ServiceMessage error = errorServiceMessage(500, content);

    StepVerifier
        .create(websocketResource.sendMessages(Mono.just(GREETING_FAILING_MANY), ErrorData.class, TIMEOUT))
        .assertNext(msg -> assertEquals(content, msg.data()))
        .assertNext(msg -> assertEquals(content, msg.data()))
        .assertNext(msg -> assertErrorMessage(error, msg))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedYet() {
    microservicesResource.startGateway();
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    ServiceMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    Publisher<ServiceMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_ONE);

    StepVerifier.FirstStep<ServiceMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, ErrorData.class, TIMEOUT));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);
  }

  @Test
  public void testServicesNotStartedThenStarted() {
    microservicesResource.startGateway();
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    ServiceMessage error = unreachableServiceMessage(GREETING_ONE.qualifier());

    // send many requests and expect several error responses
    Publisher<ServiceMessage> requests = Flux.range(0, REQUEST_NUM).map(i -> GREETING_ONE);

    StepVerifier.FirstStep<ServiceMessage> stepVerifier = StepVerifier
        .create(websocketResource.sendMessages(requests, ErrorData.class, TIMEOUT));

    IntStream.range(0, REQUEST_NUM).forEach(i -> {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    });

    stepVerifier.expectComplete().verify(TIMEOUT);

    // start services node
    microservicesResource.startServices(microservicesResource.getGatewayAddress());

    String expectedData = "Echo:hello";

    StepVerifier
        .create(websocketResource.sendMessages(Mono.just(GREETING_ONE), String.class, TIMEOUT))
        .assertNext(msg -> assertEquals(expectedData, msg.data()))
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoOne() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    GreetingResponse expectedData = new GreetingResponse("Echo:hello");

    StepVerifier
        .create(websocketResource.sendMessages(Mono.just(GREETING_POJO_ONE), GreetingResponse.class, TIMEOUT))
        .assertNext(msg -> {
          assertThat(msg.data(), instanceOf(GreetingResponse.class));
          assertEquals(expectedData.getText(), msg.<GreetingResponse>data().getText());
        })
        .expectComplete()
        .verify(TIMEOUT);
  }

  @Test
  public void testGreetingPojoMany() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    int n = 10;
    List<GreetingResponse> expected = IntStream.range(0, n)
        .mapToObj(i -> "Greeting (" + i + ") to: hello")
        .map(GreetingResponse::new)
        .collect(Collectors.toList());

    List<GreetingResponse> actual =
        websocketResource
            .sendMessages(Mono.just(GREETING_POJO_MANY), GreetingResponse.class, TIMEOUT)
            .take(n)
            .map(ServiceMessage::data)
            .cast(GreetingResponse.class)
            .collectList().block(TIMEOUT);

    assertEquals(expected, actual);
  }

  @Test
  public void testInvalidRequest() {
    microservicesResource.startGateway();
    microservicesResource.startServices(microservicesResource.getGatewayAddress());
    websocketResource.startWebSocketServer(microservicesResource.getGateway());

    Publisher<String> requests =
        Flux.range(0, REQUEST_NUM).map(i -> "q=/invalid/qualifier;data=invalid_message");

    ServiceMessage error = errorServiceMessage(400, "Failed to decode message headers {headers=0, data=0}");

    StepVerifier.FirstStep<ServiceMessage> stepVerifier =
        StepVerifier.create(websocketResource.sendPayloads(requests, ErrorData.class, TIMEOUT));

    for (int i = 0; i < REQUEST_NUM; i++) {
      stepVerifier.assertNext(msg -> assertErrorMessage(error, msg));
    }

    stepVerifier.expectComplete().verify(TIMEOUT);
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

  private void assertErrorMessage(ServiceMessage expected, ServiceMessage actual) {
    assertEquals(expected.qualifier(), actual.qualifier());
    assertThat(actual.data(), instanceOf(ErrorData.class));
    ErrorData expectedData = expected.data();
    ErrorData actualData = actual.data();
    assertEquals(expectedData.getErrorCode(), actualData.getErrorCode());
    assertEquals(expectedData.getErrorMessage(), actualData.getErrorMessage());
  }
}
